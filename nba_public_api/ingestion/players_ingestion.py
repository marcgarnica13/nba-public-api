from nba_public_api.ingestion.nba import StatsNBAPipeline
from nba_public_api.io import config


class PlayersIngestion(StatsNBAPipeline):

    def __init__(self):
        super().__init__(endpoints=["commonallplayers"])
