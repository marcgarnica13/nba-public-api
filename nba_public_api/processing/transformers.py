import json

import numpy as np


def unify_headers_and_rows(response, *args, **kwargs):
    try:
        payload = response.json()
        headers = payload.get("resultSets", [{}])[0].get("headers", [])
        rows = payload.get("resultSets", [{}])[0].get("rowSet", [])
        new_payload = [dict(zip(headers, row)) for row in np.array(rows)]
        modified_content: bytes = json.dumps(new_payload).encode("utf-8")
        response._content = modified_content
    except KeyError as e:
        raise KeyError(f"Error in parsing response: {e}")
    except Exception as e:
        raise ValueError(f"Error in parsing response: {e}")

    return response
