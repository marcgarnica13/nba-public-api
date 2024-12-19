from nba_public_api.ingestion.nba import NBAPipeline
from nba_public_api.ingestion.players import PlayersPipeline
from nba_public_api.ingestion.players_regular_stats_by_season import (
    PlayersRegularStatsBySeason,
)

nba = NBAPipeline(
    endpoints=["commonallplayers"],
    table_name="players",
).run(IsOnlyCurrentSeason=1, LeagueID="00", Season="2022-23")

nba_players = PlayersPipeline().run(
    IsOnlyCurrentSeason=1, LeagueID="00", Season="2022-23"
)

nba_players_stats = PlayersRegularStatsBySeason().run(
    LeagueID="00", PerMode="Totals", PlayerID=201142
)

import duckdb

conn = duckdb.connect("nba_ingestion.duckdb")
print("Loaded tables: ")
conn.sql("show all tables")
conn.sql("select * from dbt.teams")
conn.close()

from dlt.sources.helpers.rest_client import RESTClient

source_client = RESTClient(base_url="https://stats.gleague.nba.com/stats")
response = source_client.get(
    "/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=2022-23"
)

import dlt

pipeline = dlt.pipeline(
    pipeline_name="nba_ingestion", destination="duckdb", dataset_name="nba_data.staging"
)

venv = dlt.dbt.get_venv(pipeline)

dbt = dlt.dbt.package(pipeline, "../nba_public_api/models", venv=venv)

models = dbt.run_all()

# show outcome
for m in models:
    print(
        f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}"
    )
