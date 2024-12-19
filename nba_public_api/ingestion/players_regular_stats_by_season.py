from nba_public_api.ingestion.nba import NBAPipeline
from nba_public_api.processing.transformers import normalize_to_columnar


class PlayersRegularStatsBySeason(NBAPipeline):

    def __init__(self, **kwargs):
        super().__init__(
            endpoints=["playercareerstats"],
            table_name="PlayerRegularSeasonStats",
            **kwargs
        )

    def get_resource_defaults(self):
        return {
            "endpoint": {
                "response_actions": [
                    {"status_code": 200, "action": normalize_to_columnar}
                ]
            }
        }
