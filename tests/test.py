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

conn = duckdb.connect("nba_players_stats_ingestion.duckdb")
print("Loaded tables: ")
conn.sql("show all tables")
conn.sql("select * from nba_data._dlt_version")
conn.close()

from dlt.sources.helpers.rest_client import RESTClient

source_client = RESTClient(base_url="https://stats.gleague.nba.com/stats")
response = source_client.get(
    "/commonallplayers?IsOnlyCurrentSeason=1&LeagueID=00&Season=2022-23"
)
