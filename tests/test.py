from nba_public_api.dlt_pipelines.players_ingestion import StatsNBAPipeline

from dlt.sources.helpers.rest_client

StatsNBAPipeline(endpoints=["commonallplayers"]).run(
    IsOnlyCurrentSeason=1, LeagueID="00", Season="2022-23"
)

import duckdb

conn = duckdb.connect("nba_ingestion.duckdb")
print("Loaded tables: ")
conn.sql("show all tables")
conn.close()

8mECCnu5MA9woQ
zJ8QQWROX5mgBg