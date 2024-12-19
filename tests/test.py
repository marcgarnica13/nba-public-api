from nba_public_api.ingestion.players_ingestion import StatsNBAPipeline

nba = StatsNBAPipeline(endpoints=["commonallplayers"], table_name = "players").run(
    IsOnlyCurrentSeason=1, LeagueID="00", Season="2022-23"
)

import duckdb

conn = duckdb.connect("nba_ingestion.duckdb")
print("Loaded tables: ")
conn.sql("show all tables")
conn.close()

8mECCnu5MA9woQ
zJ8QQWROX5mgBg


from dlt.sources.helpers.rest_client import RESTClient
source_client = RESTClient(base_url="https://stats.gleague.nba.com/stats")
response = source_client.get("/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=2022-23")  

import dbt
import dlt

venv = dlt.dbt.get_venv(nba.pipeline)

models = dbt.run_all()