from nba_public_api.ingestion.nba import NBAPipeline
from nba_public_api.processing.transformers import unify_headers_and_rows


class PlayersPipeline(NBAPipeline):

    def __init__(self, **kwargs):
        super().__init__(endpoints=["commonallplayers"], table_name="players", **kwargs)

    def get_resource_defaults(self):
        return {
            "endpoint": {
                "response_actions": [
                    {"status_code": 200, "action": unify_headers_and_rows}
                ]
            }
        }
