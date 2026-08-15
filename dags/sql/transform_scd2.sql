-- ============================================================
-- M3: SCD2-transformasjon  (staging_enheter -> enheter_historikk)
-- ------------------------------------------------------------
-- Sammenligner gjeldende staging mot gjeldende historikk og
-- bygger Slowly Changing Dimension Type 2:
--
--   NY enhet      -> sett inn ny gjeldende rad
--   ENDRET enhet  -> lukk gammel rad + sett inn ny gjeldende rad
--   SLETTET enhet -> lukk gjeldende rad, åpne ingen ny
--   UENDRET       -> gjør ingenting (ingen støy-rader)
--
-- Hele skriptet er idempotent: kjør det to ganger på rad, og
-- andre kjøring gjør ingenting, fordi da er staging og historikk
-- allerede i samsvar.
-- ============================================================
-- MERK: BEGIN/COMMIT er bevisst utelatt. Transformasjonen kjøres
-- i én transaksjon fra transform_scd2-tasken (engine.begin), slik
-- at steg 1-3 enten alle lykkes eller alle rulles tilbake.
-- ============================================================

-- 1) SLETTINGER: lukk gjeldende historikk-rad for enheter som
--    er markert slettet i staging og fortsatt står som gjeldende.
UPDATE enheter_historikk h
SET valid_to     = now(),
    er_gjeldende = FALSE
FROM staging_enheter s
WHERE s.organisasjonsnummer = h.organisasjonsnummer
  AND h.er_gjeldende
  AND s.er_slettet;

-- 2) ENDRINGER: lukk gjeldende rad for enheter der ett eller
--    flere forretningsfelt har endret seg (og som IKKE er slettet).
UPDATE enheter_historikk h
SET valid_to     = now(),
    er_gjeldende = FALSE
FROM staging_enheter s
WHERE s.organisasjonsnummer = h.organisasjonsnummer
  AND h.er_gjeldende
  AND NOT s.er_slettet
  AND (
        COALESCE(s.navn,'')               <> COALESCE(h.navn,'')
     OR COALESCE(s.organisasjonsform,'')  <> COALESCE(h.organisasjonsform,'')
     OR COALESCE(s.naeringskode,'')       <> COALESCE(h.naeringskode,'')
     OR COALESCE(s.forretningsadresse,'') <> COALESCE(h.forretningsadresse,'')
     OR COALESCE(s.postnummer,'')         <> COALESCE(h.postnummer,'')
     OR COALESCE(s.poststed,'')           <> COALESCE(h.poststed,'')
     OR COALESCE(s.konkurs,FALSE)         <> COALESCE(h.konkurs,FALSE)
     OR COALESCE(s.under_avvikling,FALSE) <> COALESCE(h.under_avvikling,FALSE)
  );

-- 3) NYE + ENDREDE: sett inn ny gjeldende rad for enheter som
--    enten er helt nye eller nettopp fikk sin gamle rad lukket
--    i steg 2. Betingelse: finnes i staging, ikke slettet, og
--    har ingen gjeldende rad i historikk akkurat nå.
INSERT INTO enheter_historikk (
    organisasjonsnummer, navn, organisasjonsform, naeringskode,
    forretningsadresse, postnummer, poststed,
    konkurs, under_avvikling,
    valid_from, valid_to, er_gjeldende
)
SELECT
    s.organisasjonsnummer, s.navn, s.organisasjonsform, s.naeringskode,
    s.forretningsadresse, s.postnummer, s.poststed,
    s.konkurs, s.under_avvikling,
    now(), NULL, TRUE
FROM staging_enheter s
WHERE NOT s.er_slettet
  AND NOT EXISTS (
        SELECT 1 FROM enheter_historikk h
        WHERE h.organisasjonsnummer = s.organisasjonsnummer
          AND h.er_gjeldende
  );
