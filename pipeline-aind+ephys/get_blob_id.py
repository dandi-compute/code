import argparse

import dandi.dandiapi

def get_blob_id(*, token: str, dandiset_id: str, path: str) -> str:
    client = dandi.dandiapi.DandiAPIClient(token=token)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)
    asset = dandiset.get_asset_by_path(path=path)
    return asset.blob

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get blob id for an asset in a Dandiset.")
    parser.add_argument("token", help="DANDI API token.")
    parser.add_argument("dandiset", help="Dandiset id.")
    parser.add_argument("path", help="Asset path within the Dandiset.")
    args = parser.parse_args()

    blob_id = get_blob_id(token=args.token, dandiset_id=args.dandiset, path=args.path)
    print(blob_id)
