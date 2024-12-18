import dlt
from dlt.sources.rest_api import rest_api_source

from nba_public_api.io import config


class StatsNBAPipeline:

    def __init__(self, endpoints: list[str], pipeline_name: str = "nba_ingestion"):
        self.ENDPOINTS = endpoints
        self.pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name="nba_data",
            refresh="drop_sources",
            progress="log",
        )

    def configure_source(self, **kwargs):
        api_config = config.load_api_spec()
        rest_api_config = {
            "client": {"base_url": api_config.get("servers", [{}])[0].get("url")},
            "resources": [],
            "resource_defaults": {"endpoint": {"data_selector": "resultSets.headers"}},
        }
        for endpoint in self.ENDPOINTS:
            api_path = "/" + endpoint
            if api_path not in api_config.get("paths", {}):
                raise ValueError(f"API path not found in config: {api_path}")
            api_path_config = api_config.get("paths", {}).get(api_path, {})
            api_endpoint = {"path": endpoint, "method": "GET", "params": {}}
            for parameter in api_path_config.get("get", {}).get("parameters", []):
                if "$ref" not in parameter:
                    raise ValueError(f"Parameter reference not found in OpenAPI spec")
                try:
                    parameter_name = parameter["$ref"].split("/")[-1]
                    parameter_value = kwargs[parameter_name]
                except KeyError:
                    raise ValueError(
                        f"Missing parameter {parameter_name} to call the API"
                    )
                except Exception as e:
                    raise ValueError(f"Error in parsing parameter: {e}")
                api_endpoint["params"][parameter_name] = parameter_value
            rest_api_config["resources"].append(
                {"name": endpoint, "endpoint": api_endpoint}
            )
        self.source = rest_api_source(rest_api_config)

    def run(self, **kwargs):
        self.configure_source(**kwargs)
        load_info = self.pipeline.run(self.source)
        return load_info
