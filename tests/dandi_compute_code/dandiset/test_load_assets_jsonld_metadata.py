import json
from unittest import mock

from dandi_compute_code import dandiset
from dandi_compute_code.dandiset._load_assets_jsonld_metadata import AssetsJsonldMetadata, load_assets_jsonld_metadata


class _FakeResponse:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = lines

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        return None

    def __iter__(self):
        return iter(self._lines)


def test_load_assets_jsonld_metadata_returns_indexed_model() -> None:
    load_assets_jsonld_metadata.cache_clear()
    line = json.dumps(
        {
            "path": "sub-test/sub-test_ecephys.nwb",
            "contentSize": "123",
            "dateModified": "2026-01-01T00:00:00+00:00",
            "contentUrl": ["https://example.test/blobs/content-id-123?download=1"],
        }
    ).encode("utf-8")
    with mock.patch(
        "dandi_compute_code.dandiset._load_assets_jsonld_metadata.urllib.request.urlopen",
        return_value=_FakeResponse([line]),
    ):
        metadata = load_assets_jsonld_metadata()

    assert isinstance(metadata, AssetsJsonldMetadata)
    assert metadata.path_to_content_id["sub-test/sub-test_ecephys.nwb"] == "content-id-123"
    assert metadata.path_to_date_modified["sub-test/sub-test_ecephys.nwb"] == "2026-01-01T00:00:00+00:00"
    assert "sub-test/sub-test_ecephys.nwb" in metadata.all_paths
    assert metadata.content_id_to_asset["content-id-123"]["contentSize"] == 123


def test_load_assets_jsonld_metadata_is_publicly_exported() -> None:
    assert dandiset.load_assets_jsonld_metadata is load_assets_jsonld_metadata
    assert dandiset.AssetsJsonldMetadata is AssetsJsonldMetadata
