-- ============================================================
-- M3: utvid staging med slette-informasjon
-- ------------------------------------------------------------
-- En slettet enhet gir 200 OK med respons_klasse="SlettetEnhet"
-- og et slettedato-felt. Vi trenger disse i staging for at
-- SCD2-transformasjonen skal kunne lukke historikk-rader for
-- slettede enheter (i stedet for å upserte NULL-er).
-- ============================================================
ALTER TABLE staging_enheter
    ADD COLUMN IF NOT EXISTS er_slettet   BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS slettedato   DATE;
