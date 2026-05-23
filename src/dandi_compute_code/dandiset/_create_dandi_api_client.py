import dandi.dandiapi

from ._globals import _SANDBOX_API_URL, _SANDBOX_DANDISET_ID


def _create_dandi_api_client(*, api_token: str, dandiset_id: str) -> dandi.dandiapi.DandiAPIClient:
    """Create a DANDI API client, routing known sandbox dandiset IDs to the sandbox API."""
    if dandiset_id == _SANDBOX_DANDISET_ID:
        return dandi.dandiapi.DandiAPIClient(api_url=_SANDBOX_API_URL)
    return dandi.dandiapi.DandiAPIClient(token=api_token)
