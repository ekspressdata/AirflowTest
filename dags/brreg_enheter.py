"""
M3: SCD2-historikk.

Endring fra M2: vi legger til et transform-steg som bygger ekte
historikk i enheter_historikk (SCD Type 2), i stedet for bare å
overskrive staging. Lagdelt arkitektur:

  staging_enheter    = landingssone, alltid siste versjon (rådata)
  enheter_historikk  = historisert lag, én rad per versjon over tid

Pipeline:
  hent_oppdateringer -> hent_og_last (til staging)
                     -> transform_scd2 (staging -> historikk, ren SQL)
                     -> lagre_hoyvann

SCD2-logikken bor i dags/sql/transform_scd2.sql - ren, idempotent SQL.
Se den fila for selve historiseringen (ny/endret/slettet/uendret).

Slettinger: en slettet enhet gir 200 OK med respons_klasse=
"SlettetEnhet" og et slettedato-felt (bekreftet mot API-et).
hent_og_last fanger dette til staging (er_slettet=TRUE), og
transform-steget lukker da historikk-raden uten å åpne en ny.
"""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task

# ------------------------------------------------------------
# Konfigurasjon
# ------------------------------------------------------------
BASE = "https://data.brreg.no/enhetsregisteret/api"
OPPDATERINGER_URL = f"{BASE}/oppdateringer/enheter"
KILDE = "enheter"
MAKS_PER_KJORING = 200
DB_CONN = os.environ["BRREG_DB_CONN"]
FORSTE_DATO = "2026-08-12T00:00:00.000Z"

# sti til SQL-fila (ligger ved siden av denne DAG-fila)
SQL_DIR = Path(__file__).parent / "sql"


@dag(
    dag_id="brreg_enheter",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Oslo"),
    catchup=False,
    max_active_runs=1,
    tags=["brreg", "m3"],
)
def brreg_enheter():

    @task
    def hent_oppdateringer() -> dict:
        """Hent endringer siden forrige high-water mark (oppdateringsid)."""
        from sqlalchemy import create_engine, text

        engine = create_engine(DB_CONN)
        with engine.begin() as conn:
            rad = conn.execute(
                text("SELECT siste_id FROM etl_hoyvann WHERE kilde = :k"),
                {"k": KILDE},
            ).fetchone()
        siste_id = rad[0] if rad and rad[0] is not None else None

        params = {"size": MAKS_PER_KJORING}
        if siste_id is not None:
            params["oppdateringsid"] = siste_id + 1
            print(f"Henter oppdateringer med oppdateringsid >= {siste_id + 1}")
        else:
            params["dato"] = FORSTE_DATO
            print(f"Første kjøring - henter oppdateringer fra {FORSTE_DATO}")

        resp = requests.get(OPPDATERINGER_URL, params=params,
                            headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        oppdateringer = data.get("_embedded", {}).get("oppdaterteEnheter", [])
        if not oppdateringer:
            print("Ingen nye oppdateringer.")
            return {"orgnr": [], "ny_hoyvann": siste_id}

        orgnr = []
        ny_hoyvann = siste_id or 0
        for o in oppdateringer:
            orgnr.append(o["organisasjonsnummer"])
            ny_hoyvann = max(ny_hoyvann, o["oppdateringsid"])
        orgnr = sorted(set(orgnr))
        print(f"{len(oppdateringer)} oppdateringer -> {len(orgnr)} unike enheter. "
              f"Ny high-water mark: {ny_hoyvann}")
        return {"orgnr": orgnr, "ny_hoyvann": ny_hoyvann}

    @task
    def hent_og_last(**context) -> int:
        """
        Hent hver endret enhet på nytt og upsert til staging.
        Fanger nå OGSÅ slettinger: respons_klasse=="SlettetEnhet"
        -> er_slettet=TRUE + slettedato, slik at transform-steget
        kan lukke historikk-raden.

        Henter XCom eksplisitt via context (bevisst læringsgrep -
        viser hva TaskFlow ellers skjuler).
        """
        from sqlalchemy import create_engine, text

        ti = context["ti"]
        payload = ti.xcom_pull(task_ids="hent_oppdateringer")   # eksplisitt XCom
        orgnr_liste = payload["orgnr"]
        if not orgnr_liste:
            print("Ingenting å laste.")
            return 0

        engine = create_engine(DB_CONN)
        upsert = text("""
            INSERT INTO staging_enheter (
                organisasjonsnummer, navn, organisasjonsform,
                naeringskode, naeringsbeskrivelse, forretningsadresse,
                postnummer, poststed, kommunenummer,
                konkurs, under_avvikling, registreringsdato,
                er_slettet, slettedato
            ) VALUES (
                :orgnr, :navn, :orgform,
                :nkode, :nbesk, :adresse,
                :postnr, :poststed, :kommunenr,
                :konkurs, :avvikling, :regdato,
                :slettet, :slettedato
            )
            ON CONFLICT (organisasjonsnummer) DO UPDATE SET
                navn                = EXCLUDED.navn,
                organisasjonsform   = EXCLUDED.organisasjonsform,
                naeringskode        = EXCLUDED.naeringskode,
                naeringsbeskrivelse = EXCLUDED.naeringsbeskrivelse,
                forretningsadresse  = EXCLUDED.forretningsadresse,
                postnummer          = EXCLUDED.postnummer,
                poststed            = EXCLUDED.poststed,
                kommunenummer       = EXCLUDED.kommunenummer,
                konkurs             = EXCLUDED.konkurs,
                under_avvikling     = EXCLUDED.under_avvikling,
                registreringsdato   = EXCLUDED.registreringsdato,
                er_slettet          = EXCLUDED.er_slettet,
                slettedato          = EXCLUDED.slettedato,
                hentet_tidspunkt    = now()
        """)

        headers = {"Accept": "application/json"}
        rader = 0
        with engine.begin() as conn:
            for orgnr in orgnr_liste:
                r = requests.get(f"{BASE}/enheter/{orgnr}", headers=headers, timeout=30)
                # ekte "fjernet av juridiske årsaker" -> 410. Behandles som slettet.
                if r.status_code == 410:
                    conn.execute(upsert, _slettet_rad(orgnr))
                    rader += 1
                    continue
                if r.status_code == 404:
                    print(f"  {orgnr}: 404, hoppet over")
                    continue
                r.raise_for_status()
                e = r.json()

                er_slettet = e.get("respons_klasse") == "SlettetEnhet" \
                             or e.get("slettedato") is not None

                fadr = e.get("forretningsadresse", {}) or {}
                adr_liste = fadr.get("adresse", []) or []
                nkode = e.get("naeringskode1", {}) or {}
                conn.execute(upsert, {
                    "orgnr":      e.get("organisasjonsnummer"),
                    "navn":       e.get("navn"),
                    "orgform":    (e.get("organisasjonsform", {}) or {}).get("kode"),
                    "nkode":      nkode.get("kode"),
                    "nbesk":      nkode.get("beskrivelse"),
                    "adresse":    ", ".join(adr_liste),
                    "postnr":     fadr.get("postnummer"),
                    "poststed":   fadr.get("poststed"),
                    "kommunenr":  fadr.get("kommunenummer"),
                    "konkurs":    e.get("konkurs"),
                    "avvikling":  e.get("underAvvikling"),
                    "regdato":    e.get("registreringsdatoEnhetsregisteret"),
                    "slettet":    er_slettet,
                    "slettedato": e.get("slettedato"),
                })
                rader += 1
        print(f"Upsertet {rader} enheter til staging_enheter")
        return rader

    @task
    def transform_scd2(antall_lastet: int) -> None:
        """
        Kjør SCD2-transformasjonen: staging -> historikk.
        Ren SQL fra dags/sql/transform_scd2.sql.

        HELE transformasjonen kjøres i ÉN transaksjon (engine.begin).
        Det er avgjørende for SCD2-garantien: lukking av gammel rad
        (steg 2) og innsetting av ny (steg 3) må enten begge lykkes
        eller begge rulles tilbake - ellers kan en enhet stå uten
        gjeldende rad. Idempotent: kjør to ganger, andre gjør ingenting.
        """
        from sqlalchemy import create_engine, text

        sql = (SQL_DIR / "transform_scd2.sql").read_text(encoding="utf-8")
        # fjern BEGIN;/COMMIT; fra fila - vi styrer transaksjonen her
        setninger = _split_sql(sql)

        engine = create_engine(DB_CONN)
        with engine.begin() as conn:                 # <-- én transaksjon
            for setning in setninger:
                conn.execute(text(setning))
            gjeldende = conn.execute(text(
                "SELECT count(*) FROM enheter_historikk WHERE er_gjeldende"
            )).scalar()
            totalt = conn.execute(text(
                "SELECT count(*) FROM enheter_historikk"
            )).scalar()
        print(f"SCD2 ferdig. Historikk: {totalt} rader totalt, "
              f"{gjeldende} gjeldende.")

    @task
    def lagre_hoyvann(payload: dict) -> None:
        """Skriv ny high-water mark til slutt (etter vellykket transform)."""
        from sqlalchemy import create_engine, text
        ny_hoyvann = payload["ny_hoyvann"]
        if ny_hoyvann is None:
            print("Ingen high-water mark å lagre.")
            return
        engine = create_engine(DB_CONN)
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_hoyvann (kilde, siste_id, siste_tidspunkt)
                VALUES (:k, :id, now())
                ON CONFLICT (kilde) DO UPDATE SET
                    siste_id        = EXCLUDED.siste_id,
                    siste_tidspunkt = EXCLUDED.siste_tidspunkt
            """), {"k": KILDE, "id": ny_hoyvann})
        print(f"High-water mark lagret: oppdateringsid = {ny_hoyvann}")

    # --- avhengigheter ---
    oppdateringer = hent_oppdateringer()
    antall = hent_og_last()
    oppdateringer >> antall              # rekkefølge (XCom hentes eksplisitt)
    transform = transform_scd2(antall)   # transform etter last
    lagre_hoyvann(oppdateringer) << transform   # high-water helt til slutt


# ------------------------------------------------------------
# Hjelpefunksjoner
# ------------------------------------------------------------
def _slettet_rad(orgnr: str) -> dict:
    """Minimal staging-rad for en enhet som er fjernet (410)."""
    return {
        "orgnr": orgnr, "navn": None, "orgform": None, "nkode": None,
        "nbesk": None, "adresse": None, "postnr": None, "poststed": None,
        "kommunenr": None, "konkurs": None, "avvikling": None, "regdato": None,
        "slettet": True, "slettedato": None,
    }


def _split_sql(skript: str) -> list[str]:
    """
    Del SQL-skriptet i enkeltsetninger (psycopg2 kjører én om gangen).
    Fjerner kommentarlinjer og evt. BEGIN/COMMIT - transaksjonen styres
    av kalleren (transform_scd2 via engine.begin), så alle setningene
    kjører i samme transaksjon.
    """
    linjer = []
    for linje in skript.splitlines():
        stripped = linje.strip()
        if stripped.startswith("--") or not stripped:
            continue
        if stripped.upper() in ("BEGIN;", "COMMIT;"):
            continue
        linjer.append(linje)
    tekst = "\n".join(linjer)
    return [s.strip() for s in tekst.split(";") if s.strip()]


brreg_enheter()
