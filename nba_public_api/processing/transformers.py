import json

import numpy as np


def _unify_headers_and_rows(headers: list, rows: list) -> list:
    """
    Unifies headers and rows into a list of dictionaries.

    Args:
        headers (list): A list of header names.
        rows (list): A list of rows, where each row is a list of values.

    Returns:
        list: A list of dictionaries, where each dictionary represents a row with header-value pairs.
    """
    return [dict(zip(headers, row)) for row in np.array(rows)]


def unify_headers_and_rows(response, *args, **kwargs):
    """
    Unifies headers and rows from the response into a list of dictionaries and modifies the response content.

    Args:
        response: The response object containing the JSON payload.
        *args: Additional arguments of the request.
        **kwargs: Additional keyword arguments of the request.

    Returns:
        response: The modified response object with unified headers and rows.
    """
    try:
        payload = response.json()
        headers = payload.get("resultSets", [{}])[0].get("headers", [])
        rows = payload.get("resultSets", [{}])[0].get("rowSet", [])
        new_payload = _unify_headers_and_rows(headers=headers, rows=rows)
        modified_content: bytes = json.dumps(new_payload).encode("utf-8")
        response._content = modified_content
    except KeyError as e:
        raise KeyError(f"Error in parsing response: {e}")
    except Exception as e:
        raise ValueError(f"Error in parsing response: {e}")

    return response


def _normalize_to_columnar(
    headers: list, rows: list, row_values: list = [], column_prefix: str = ""
) -> list:
    list_of_objects = []
    for row in rows:
        row_data_objects = []
        new_data_object = {}
        for header_idx, header in enumerate(headers):
            if header in row_values:
                new_data_object[header] = row[header_idx]
            else:
                new_data_object[f"{column_prefix}_key"] = header
                new_data_object[f"{column_prefix}_value"] = row[header_idx]
                row_data_objects.append(new_data_object.copy())
        list_of_objects += row_data_objects
    return list_of_objects


def normalize_to_columnar(response, *args, **kwargs):
    try:
        payload = response.json()
        headers = payload.get("resultSets", [{}])[0].get("headers", [])
        rows = payload.get("resultSets", [{}])[0].get("rowSet", [])
        new_payload = _normalize_to_columnar(
            headers=headers,
            rows=rows,
            row_values=[
                "PLAYER_ID",
                "SEASON_ID",
                "LEAGUE_ID",
                "TEAM_ID",
                "TEAM_ABBREVIATION",
            ],
            column_prefix="STAT",
        )
        modified_content: bytes = json.dumps(new_payload).encode("utf-8")
        response._content = modified_content
    except KeyError as e:
        raise KeyError(f"Error in parsing response: {e}")
    except Exception as e:
        raise ValueError(f"Error in parsing response: {e}")

    return response
