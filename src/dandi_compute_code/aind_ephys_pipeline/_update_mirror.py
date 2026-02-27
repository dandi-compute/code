import gzip
import json
import os
import pathlib
import tempfile

import dandi.dandiapi
import dandi.download
import dandi.move
import pydantic
import requests


@pydantic.validate_call
def update_mirror() -> None:
    """
    Update `001675` (public, human-readable view of AIND only) to latest values of unique ID references.

    Update any new results from backend `001697` (private, dev-only view of all pipelines) as well.
    """
    tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="/orcd/data/dandi/001/dandi-compute/tmp"))
    print(f"{tmpdir=}")
    dandi.download.download(
        urls="DANDI:001675",
        output_dir=tmpdir,
        get_metadata=True,
        get_assets=False,
    )

    content_id_to_unique_dandiset_path_url = (
        "https://raw.githubusercontent.com/dandi-cache/content-id-to-unique-dandiset-path/"
        "refs/heads/min/derivatives/content_id_to_unique_dandiset_path.min.json.gz"
    )
    response = requests.get(content_id_to_unique_dandiset_path_url)
    if response.status_code != 200:
        message = (
            f"Failed to retrieve content ID to unique path mapping from {content_id_to_unique_dandiset_path_url} - "
            f"status code {response.status_code}: {response.json()}"
        )
        raise RuntimeError(message)
    content_id_to_unique_dandiset_path = json.loads(gzip.decompress(data=response.content))

    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    source_dandiset = client.get_dandiset(dandiset_id="001697")
    # target_dandiset = client.get_dandiset(dandiset_id="001675")

    default_paths = [
        "derivatives/pipeline-aind+ephys/CHANGES.md",
        "derivatives/pipeline-aind+ephys/README.md",
        "derivatives/pipeline-aind+ephys/dataset_description.json",
    ]
    for default_path in default_paths:
        source_asset = next(source_dandiset.get_assets_with_path_prefix(path=default_path), None)
        if source_asset is not None:
            source_asset.download(filepath=tmpdir / source_asset.path)

    # TODO: historical issue caused by taking Yarik's suggestion...
    temporary_shorthand = {
        "048d1ee9": "048d1ee9-83b7-491f-8f02-1ca615b1d455",
        # "1edadfd6": "",
        "28a2b0b6": "28a2b0b6-0dbf-4664-a358-e796dd924b95",
        "baec93f8": "baec93f8-8ba7-4d37-bc39-dd322d192bd0",
        "e6fc49d5": "e6fc49d5-5f13-4bf2-84e9-b971ed65184e",
        # "": ""
    }
    temporary_shorthand_inv = {v: k for k, v in temporary_shorthand.items()}
    asset_prefix = "derivatives/pipeline-aind+ephys/derivatives/asset-"
    derivative_assets = source_dandiset.get_assets_with_path_prefix(
        path=asset_prefix
    )  # TODO: this is quite inefficient
    processed_assets = {
        identifier
        for asset in derivative_assets
        if (identifier := asset.path.removeprefix(asset_prefix).split("/")[0]) != "1edadfd6"
    }
    full_processed_assets = {temporary_shorthand[short_id] for short_id in processed_assets}

    previous_known_paths = {
        asset: content_id_to_unique_dandiset_path.get(asset, None)
        for asset in full_processed_assets
        if (dandiset_path := content_id_to_unique_dandiset_path.get(asset, None)) is not None
    }
    for asset, dandiset_path in previous_known_paths.items():
        results_prefix = f"derivatives/pipeline-aind+ephys/derivatives/asset-{temporary_shorthand_inv[asset]}"
        all_results_paths = list(source_dandiset.get_assets_with_path_prefix(path=results_prefix))
        possible_results = [asset for asset in all_results_paths if "output" in asset.path]

        dandiset, path_in_dandiset = next(iter(dandiset_path.items()))

        path_parent = pathlib.Path(path_in_dandiset.removeprefix("sourcedata/")).parent
        subdir = tmpdir / "001675" / "derivatives" / f"dandiset-{dandiset}" / path_parent
        subdir.mkdir(parents=True, exist_ok=True)

        subdir
        possible_results


if __name__ == "__main__":
    update_mirror()
