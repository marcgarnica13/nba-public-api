import dlt
from dlt.sources.rest_api import rest_api_source

from nba_public_api.io import config
from nba_public_api.processing.transformers import unify_headers_and_rows


class NBAPipeline:
    """A pipeline class for ingesting NBA statistics data from specified API endpoints into a DuckDB database.

    Attributes:
        ENDPOINTS (list[str]): A list of API endpoints to be ingested.
        pipeline (dlt.Pipeline): The data loading pipeline configured for DuckDB.
        append_load_info (bool): Flag indicating whether to append load information to the data lineage table.
        data_lineage_table_name (str): The name of the table to store data lineage information.
    """

    def __init__(
        self,
        endpoints: list[str],
        table_name: str,
        pipeline_name: str = "nba_ingestion",
        **kwargs,
    ):
        self.ENDPOINTS = endpoints
        self.table_name = table_name
        self.write_disposition = "append"
        self.pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=kwargs.get("pipeline_destination", "duckdb"),
            dataset_name=kwargs.get("pipeline_dataset_name", "nba_data.raw"),
            refresh=kwargs.get("pipeline_refresh", None),
            progress=kwargs.get("pipeline_progress", "log"),
        )
        self.append_load_info = kwargs.get("append_load_info", True)
        self.data_lineage_table_name = kwargs.get(
            "data_lineage_table_name", "data_lineage"
        )

    def configure_source(self, **kwargs):
        """
        Configures the source for the NBA API by loading the API specification and setting up the REST API configuration.

        This method reads the API specification, constructs the REST API configuration, and sets up the endpoints
        with the necessary parameters. It raises errors if required parameters are missing or if there are issues
        with the API specification.

        Args:
            **kwargs: Arbitrary keyword arguments representing the parameters required for the API endpoints.

        Raises:
            ValueError: If the API path is not found in the configuration, if a parameter reference is not found
                in the OpenAPI spec, if a required parameter is missing, or if there is an error in parsing
                a parameter.

        Example:
            configure_source(param1=value1, param2=value2, ...)
        """
        api_config = config.load_api_spec()
        rest_api_config = {
            "client": {"base_url": api_config.get("servers", [{}])[0].get("url")},
            "resources": [],
            "resource_defaults": self.get_resource_defaults(),
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
        self.source = rest_api_source(rest_api_config, name="nba_api")

    def get_resource_defaults(self) -> dict:
        return {}

    def run(self, **kwargs):
        """
        Executes the pipeline by configuring the source and running the pipeline with the provided arguments.

        Args:
            **kwargs: Arbitrary keyword arguments passed to the source configuration.

        Returns:
            load_info: An object containing information about the load process, including load packages.
        """
        self.configure_source(**kwargs)
        load_info = self.pipeline.run(
            self.source,
            table_name=self.table_name,
            write_disposition=self.write_disposition,
        )
        self.pipeline.run(
            load_info.load_packages,
            table_name=self.data_lineage_table_name,
            write_disposition="append" if self.append_load_info else "replace",
        )
        return load_info
