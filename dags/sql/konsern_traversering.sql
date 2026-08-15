-- ============================================================
-- M4: Rekursiv traversering av konsernhierarki
-- ------------------------------------------------------------
-- konsern_kant inneholder flate mor->datter-kanter:
--   (parent_orgnr, child_orgnr, grunnlag)
--
-- Denne CTE-en traverserer hierarkiet NEDOVER fra hver topp-mor
-- (en enhet som er parent, men aldri child = ingen mor over seg),
-- og finner hele kjeden av datterselskaper med:
--   - nivaa            : hvor dypt nede i konsernet datteren ligger
--   - ultimate_mor     : topp-selskapet kjeden starter fra
--   - sti              : hele veien fra topp til denne noden (for innsyn)
--   - akkumulert_grunnlag : grunnlag multiplisert nedover kjeden
--
-- SYKLUSVAKT: vi bærer med en array 'besokte' av org.nr vi har
-- passert. Dukker en node opp igjen, stopper vi den grenen i
-- stedet for å loope i det uendelige. Ekte Brreg-konsern er
-- asykliske (100%-knytning), men vi stoler aldri på at inputdata
-- er garantert løkkefri - derfor vakten.
-- ============================================================

WITH RECURSIVE traversering AS (

    -- ANKER: topp-mødre. En enhet som er parent i minst én kant,
    -- men aldri selv er child (ingen mor over seg).
    -- DISTINCT fordi en topp-mor kan ha flere døtre (flere kanter),
    -- men vi vil ha nøyaktig én startnode per mor.
    SELECT DISTINCT
        k.parent_orgnr                        AS orgnr,
        k.parent_orgnr                        AS ultimate_mor,
        0                                     AS nivaa,
        ARRAY[k.parent_orgnr]                 AS besokte,
        k.parent_orgnr                        AS sti,
        1.0::numeric                          AS akkumulert_andel
    FROM konsern_kant k
    WHERE NOT EXISTS (
        SELECT 1 FROM konsern_kant k2
        WHERE k2.child_orgnr = k.parent_orgnr
    )

    UNION ALL

    -- REKURSIVT STEG: for hver node vi har nådd, følg kantene
    -- videre ned til dens døtre.
    SELECT
        k.child_orgnr,
        t.ultimate_mor,
        t.nivaa + 1,
        t.besokte || k.child_orgnr,
        t.sti || ' > ' || k.child_orgnr,
        -- grunnlag er tekst som "100%"; trekk ut tallet og multipliser
        t.akkumulert_andel * (
            NULLIF(regexp_replace(COALESCE(k.grunnlag,'100'), '[^0-9.]', '', 'g'), '')::numeric / 100.0
        )
    FROM traversering t
    JOIN konsern_kant k
      ON k.parent_orgnr = t.orgnr
    -- SYKLUSVAKT: ikke følg en kant til en node vi allerede har besøkt
    WHERE NOT (k.child_orgnr = ANY(t.besokte))
)

SELECT
    ultimate_mor,
    orgnr,
    nivaa,
    sti,
    round(akkumulert_andel * 100, 2) AS akkumulert_prosent
FROM traversering
ORDER BY ultimate_mor, nivaa, orgnr;
