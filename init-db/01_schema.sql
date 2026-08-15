-- ============================================================
-- M1: Postgres-skjema for brreg-airflow
-- ------------------------------------------------------------
-- Tre tabeller:
--   1. staging_enheter      - rå landingssone, overskrives/upsertes
--   2. enheter_historikk    - SCD2-historikk (bygges i M3)
--   3. konsern_kant         - flate mor/datter-kanter (fylles i M4)
--
-- I M1 bruker vi bare staging_enheter. De to andre opprettes nå
-- slik at skjemaet er komplett fra start, men fylles i M3/M4.
-- ============================================================

-- ------------------------------------------------------------
-- 1. STAGING: rå landingssone for enheter hentet fra Brreg
--    Én rad per organisasjonsnummer. Upsertes ved hver kjøring
--    (M2), så ingen dubletter selv om DAG-en kjøres på nytt.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_enheter (
    organisasjonsnummer   TEXT PRIMARY KEY,
    navn                  TEXT,
    organisasjonsform     TEXT,
    naeringskode          TEXT,
    naeringsbeskrivelse   TEXT,
    forretningsadresse    TEXT,
    postnummer            TEXT,
    poststed              TEXT,
    kommunenummer         TEXT,
    konkurs               BOOLEAN,
    under_avvikling       BOOLEAN,
    registreringsdato     DATE,
    -- teknisk sporingsfelt: når hentet vi denne raden inn?
    hentet_tidspunkt      TIMESTAMPTZ DEFAULT now()
);

-- ------------------------------------------------------------
-- 2. SCD2-HISTORIKK (fylles i M3)
--    Samme enhet kan ha flere rader over tid. Gyldig-fra/til
--    forteller når hver versjon var aktuell. er_gjeldende
--    peker på den nåværende raden for rask oppslag.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enheter_historikk (
    id                    BIGSERIAL PRIMARY KEY,
    organisasjonsnummer   TEXT NOT NULL,
    navn                  TEXT,
    organisasjonsform     TEXT,
    naeringskode          TEXT,
    forretningsadresse    TEXT,
    postnummer            TEXT,
    poststed              TEXT,
    konkurs               BOOLEAN,
    under_avvikling       BOOLEAN,
    valid_from            TIMESTAMPTZ NOT NULL,
    valid_to              TIMESTAMPTZ,          -- NULL = fortsatt gjeldende
    er_gjeldende          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Rask tilgang til gjeldende versjon av en enhet
CREATE INDEX IF NOT EXISTS idx_hist_orgnr_gjeldende
    ON enheter_historikk (organisasjonsnummer)
    WHERE er_gjeldende;

-- ------------------------------------------------------------
-- 3. KONSERN-KANTER (fylles i M4)
--    Flat representasjon av mor/datter-hierarkiet, én rad per
--    kant. Den rekursive CTE-en traverserer denne tabellen.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS konsern_kant (
    parent_orgnr          TEXT NOT NULL,
    child_orgnr           TEXT NOT NULL,
    grunnlag              TEXT,                 -- f.eks. "100%"
    PRIMARY KEY (parent_orgnr, child_orgnr)
);

-- ------------------------------------------------------------
-- Hjelpetabell: high-water mark for inkrementell last (M2)
--    Lagrer siste behandlede tidspunkt/id, slik at neste
--    kjøring bare henter det som er endret siden sist.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_hoyvann (
    kilde                 TEXT PRIMARY KEY,     -- f.eks. "enheter"
    siste_tidspunkt       TIMESTAMPTZ,
    siste_id              BIGINT
);
