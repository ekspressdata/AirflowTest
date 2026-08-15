"""
M4: Konsernstruktur + rekursiv traversering.

Egen DAG, adskilt fra brreg_enheter. Ulik kadens og formål:
enheter oppdateres løpende (inkrementelt), mens konsernstruktur
endres sjelden - derfor en separat pipeline.

Pipeline:
  finn_konsern      -> finn et utvalg enheter som er i konsern
                       (erIKonsern=true) blant eiendomsselskaper
  hent_strukturer   -> for hver, hent konsernstruktur og flat ut
                       det nøstede children-treet til kanter
  last_kanter       -> upsert kantene til konsern_kant
  traverser         -> kjør den rekursive CTE-en og logg resultatet

Konsernstruktur er mor/datter-knytning (grunnlag typisk "100%"),
ikke finmaskede aksjeandeler - så den rekursive CTE-en demonstrerer
TRAVERSERING (ultimate mor, hele kjeden, nivå) mer enn andels-matte.
Syklusvakt er med uansett (se konsern_traversering.sql).
"""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task

BASE = "https://data.brreg.no/enhetsregisteret/api"
ENHETER_URL = f"{BASE}/enheter"
KONSERN_URL = f"{BASE}/konsernstruktur"
NAERINGSKODE = "68"           # fast eiendom
ANTALL_SOK = 100              # hvor mange enheter vi søker gjennom
MAKS_KONSERN = 20             # hvor mange konsern vi henter struktur for
DB_CONN = os.environ["BRREG_DB_CONN"]
SQL_DIR = Path(__file__).parent / "sql"


def _flat_ut(node: dict, kanter: list[dict]) -> None:
    """
    Rekursivt: gå gjennom det nøstede children-treet fra API-et
    og samle flate (parent, child, grunnlag)-kanter.
    """
    for barn in node.get("children", []) or []:
        parent = barn.get("parentOrganisasjonsnummer")
        child = barn.get("organisasjonsnummer")
        if parent and child:
            kanter.append({
                "parent": parent,
                "child": child,
                "grunnlag": barn.get("grunnlag"),
            })
        # gå dypere - barnet kan selv ha barn
        _flat_ut(barn, kanter)


@dag(
    dag_id="konsern_struktur",
    schedule=None,                # kjøres manuelt / sjelden
    start_date=pendulum.datetime(2026, 8, 1, tz="Europe/Oslo"),
    catchup=False,
    max_active_runs=1,
    tags=["brreg", "m4", "konsern"],
)
def konsern_struktur():

    @task
    def finn_konsern() -> list[str]:
        """Finn org.nr til eiendomsselskaper som er i konsern."""
        params = {"naeringskode": NAERINGSKODE, "size": ANTALL_SOK, "konkurs": "false"}
        resp = requests.get(ENHETER_URL, params=params,
                            headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
        enheter = resp.json().get("_embedded", {}).get("enheter", [])
        i_konsern = [
            e["organisasjonsnummer"] for e in enheter
            if e.get("erIKonsern") is True
        ][:MAKS_KONSERN]
        print(f"Fant {len(i_konsern)} konsern-selskaper å hente struktur for")
        return i_konsern

    @task
    def hent_strukturer(orgnr_liste: list[str]) -> list[dict]:
        """Hent konsernstruktur for hver og flat ut til kanter."""
        alle_kanter: list[dict] = []
        headers = {"Accept": "application/json"}
        for orgnr in orgnr_liste:
            r = requests.get(f"{KONSERN_URL}/{orgnr}", headers=headers, timeout=30)
            if r.status_code != 200:
                print(f"  {orgnr}: status {r.status_code}, hoppet over")
                continue
            struktur = r.json()
            _flat_ut(struktur, alle_kanter)
        # dedupliser (samme kant kan komme fra flere innganger)
        unike = {(k["parent"], k["child"]): k for k in alle_kanter}
        kanter = list(unike.values())
        print(f"Flatet ut til {len(kanter)} unike konsern-kanter")
        return kanter

    @task
    def last_kanter(kanter: list[dict]) -> int:
        """Upsert kantene til konsern_kant."""
        from sqlalchemy import create_engine, text
        if not kanter:
            print("Ingen kanter å laste.")
            return 0
        engine = create_engine(DB_CONN)
        upsert = text("""
            INSERT INTO konsern_kant (parent_orgnr, child_orgnr, grunnlag)
            VALUES (:parent, :child, :grunnlag)
            ON CONFLICT (parent_orgnr, child_orgnr) DO UPDATE SET
                grunnlag = EXCLUDED.grunnlag
        """)
        with engine.begin() as conn:
            for k in kanter:
                conn.execute(upsert, k)
        print(f"Upsertet {len(kanter)} kanter til konsern_kant")
        return len(kanter)

    @task
    def traverser(antall_kanter: int) -> None:
        """Kjør den rekursive CTE-en og logg resultatet."""
        from sqlalchemy import create_engine, text
        sql = (SQL_DIR / "konsern_traversering.sql").read_text(encoding="utf-8")
        engine = create_engine(DB_CONN)
        with engine.begin() as conn:
            rader = conn.execute(text(sql)).fetchall()
        print(f"Traversering ga {len(rader)} rader:")
        for r in rader[:30]:            # logg de første 30
            print(f"  mor={r.ultimate_mor} node={r.orgnr} "
                  f"nivå={r.nivaa} andel={r.akkumulert_prosent}% sti={r.sti}")
        if len(rader) > 30:
            print(f"  ... og {len(rader) - 30} rader til")

    # --- avhengigheter ---
    konsern = finn_konsern()
    strukturer = hent_strukturer(konsern)
    antall = last_kanter(strukturer)
    traverser(antall)


konsern_struktur()
