import json
from unittest import mock

from dandi_compute_code import dandiset
from dandi_compute_code.dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    load_assets_jsonld_metadata,
)


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def test_load_assets_jsonld_metadata_returns_indexed_model() -> None:
    load_assets_jsonld_metadata.cache_clear()
    payload = json.dumps(
        [
            {
                "path": "sub-test/sub-test_ecephys.nwb",
                "contentSize": "123",
                "dateModified": "2026-01-01T00:00:00+00:00",
                "contentUrl": ["https://example.test/blobs/content-id-123?download=1"],
            }
        ]
    ).encode("utf-8")
    with mock.patch(
        "dandi_compute_code.dandiset._load_assets_jsonld_metadata.urllib.request.urlopen",
        return_value=_FakeResponse(payload),
    ):
        metadata = load_assets_jsonld_metadata()

    assert isinstance(metadata, AssetsJsonldMetadata)
    assert metadata.path_to_asset_metadata["sub-test/sub-test_ecephys.nwb"] == AssetMetadata(
        path="sub-test/sub-test_ecephys.nwb",
        date_modified="2026-01-01T00:00:00+00:00",
        content_size=123,
        content_id="content-id-123",
    )
    assert metadata.content_id_to_asset["content-id-123"]["contentSize"] == 123


def test_load_assets_jsonld_metadata_is_publicly_exported() -> None:
    assert dandiset.load_assets_jsonld_metadata is load_assets_jsonld_metadata
    assert dandiset.AssetMetadata is AssetMetadata
    assert dandiset.AssetsJsonldMetadata is AssetsJsonldMetadata


def test_assets_jsonld_metadata_path_index_contains_content_size() -> None:
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            "has-all-values.nwb": AssetMetadata(
                path="has-all-values.nwb",
                date_modified="2026-01-01T00:00:00+00:00",
                content_size=11,
                content_id="content-id-1",
            ),
            "another-path.nwb": AssetMetadata(
                path="another-path.nwb",
                date_modified="2026-01-02T00:00:00+00:00",
                content_size=22,
                content_id="content-id-2",
            ),
        },
    )

    assert metadata.path_to_asset_metadata["has-all-values.nwb"] == AssetMetadata(
        path="has-all-values.nwb",
        date_modified="2026-01-01T00:00:00+00:00",
        content_size=11,
        content_id="content-id-1",
    )
    assert metadata.path_to_asset_metadata["another-path.nwb"] == AssetMetadata(
        path="another-path.nwb",
        date_modified="2026-01-02T00:00:00+00:00",
        content_size=22,
        content_id="content-id-2",
    )
