import json
import os

DEFAULT_BASE_PATH = os.path.join(os.path.dirname(__file__), "../api_specs")
BASE_PATH = os.getenv("API_DOCS_PATH", DEFAULT_BASE_PATH)


def load_api_spec(api_name: str = "nba-open-api") -> dict:
    """
    Load the API documentation JSON file based on the provided API name.

    Parameters:
    - api_name (str): The name of the API to load.

    Returns:
    - dict: The parsed JSON data from the corresponding file.

    Raises:
    - FileNotFoundError: If the specified API file does not exist.
    - json.JSONDecodeError: If the JSON file cannot be parsed.
    """
    # Construct the path to the JSON file
    file_path = os.path.join(BASE_PATH, f"{api_name}.json")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"API documentation file not found: {file_path}")

    # Load and parse the JSON file
    with open(file_path, "r", encoding="utf-8") as json_file:
        try:
            config = json.load(json_file)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to parse JSON file: {file_path}. Error: {e}"
            )

    return config


def get_path_list(api_name: str = "nba-open-api") -> list[str]:
    """
    Get the list of paths from the API documentation.

    Parameters:
    - api_name (str): The name of the API to load.

    Returns:
    - list: A list of paths available in the API documentation.
    """
    api_spec = load_api_spec(api_name)
    paths = api_spec.get("paths", {})
    return list(paths.keys())
