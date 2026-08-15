# brreg-airflow

Et lite, komplett Airflow-prosjekt som henter åpne data fra
Enhetsregisteret (Brønnøysundregistrene) og bygger en pipeline med
inkrementell last, SCD2-historikk og rekursiv konsern-traversering.

Bygget som forberedelse til teknisk intervju. Bevisst holdt lite –
poenget er korrekt implementasjon og forklarbarhet, ikke skala.

## Status

- [x] **M1** – Grunnmur: Airflow + Postgres, én DAG som fyller staging
- [ ] **M2** – Idempotens og inkrementell last (oppdaterings-API)
- [ ] **M3** – SCD2-historikk
- [ ] **M4** – Konsernstruktur + rekursiv CTE
- [ ] **M5** – Datakvalitet og polish

## Komme i gang (Windows / PowerShell)

Krever Docker Desktop kjørende.

```powershell
# fra prosjektmappen
docker compose up -d

# følg oppstarten (Ctrl+C for å slutte å følge - stopper ikke containerne)
docker compose logs -f
```

Når alt er oppe:

- **Airflow-UI:** http://localhost:8080  (bruker: `airflow` / passord: `airflow`)
- **Datadatabasen:** `localhost:5433`  (bruker: `data` / passord: `data` / db: `brreg`)

### Kjøre DAG-en

1. Åpne http://localhost:8080
2. Skru på `brreg_enheter` (bryteren til venstre for navnet)
3. Trykk ▶ (Trigger DAG) til høyre
4. Klikk deg inn på kjøringen og se `extract → load` bli grønne

### Se resultatet

```powershell
docker compose exec postgres-data psql -U data -d brreg -c "SELECT organisasjonsnummer, navn, poststed FROM staging_enheter LIMIT 10;"
docker compose exec postgres-data psql -U data -d brreg -c "SELECT count(*) FROM staging_enheter;"
```

## Stoppe

```powershell
docker compose down        # stopper containerne, beholder data
docker compose down -v     # stopper OG sletter all data (nullstill)
```

## Arkitektur

To adskilte Postgres-databaser:
- **postgres-airflow** – Airflows egen metadatabase (intern)
- **postgres-data** – vår datadatabase: `staging_enheter`,
  `enheter_historikk` (SCD2), `konsern_kant`, `etl_hoyvann`

Datakilde: Enhetsregisterets åpne API (`data.brreg.no`), NLOD-lisens,
ingen nøkkel eller søknad nødvendig.
