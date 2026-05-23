import json


def _extract_nextflow_timeline_data(*, timeline_html: str) -> dict | None:
    """Extract ``window.data`` JSON payload from a Nextflow timeline HTML report."""
    marker = "window.data ="
    marker_index = timeline_html.find(marker)
    if marker_index == -1:
        return None

    object_start = timeline_html.find("{", marker_index)
    if object_start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    object_end = -1
    for index in range(object_start, len(timeline_html)):
        character = timeline_html[index]
        if in_string:
            if escape:
                escape = False
            elif character == "\\":
                escape = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                object_end = index
                break

    if object_end == -1:
        return None

    try:
        payload = json.loads(timeline_html[object_start : object_end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None
