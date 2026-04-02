import datetime
import os
import re
import time

import dandi.dandiapi

from .aind_ephys_pipeline import prepare_aind_ephys_job, submit_aind_ephys_job

TESTING_CONTENT_ID = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
_TESTING_DANDISET_ID = "001697"
_TESTING_PREFIX = "derivatives/dandiset-214527/sub-test/sub-test_ses-aind+sample"
_DATE_PATTERN = re.compile(r"params-[^_]+_date-(\d{4}\+\d{2}\+\d{2})")


def _get_dandiset():
    """Create a fresh DANDI API client and return the testing dandiset."""
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    return client.get_dandiset(dandiset_id=_TESTING_DANDISET_ID)


def _get_testing_asset_paths(dandiset) -> list:
    """Get all testing asset paths from the dandiset."""
    return [asset.path for asset in dandiset.get_assets_with_path_prefix(path=_TESTING_PREFIX)]


def _get_all_testing_dates(asset_paths: list) -> set:
    """Extract all unique BIDS-style dates from testing asset paths."""
    return {match.group(1) for asset_path in asset_paths if (match := _DATE_PATTERN.search(asset_path))}


def _bids_date_to_python_date(date_str: str) -> datetime.date:
    """Convert a BIDS-style date string (YYYY+MM+DD) to a datetime.date object."""
    return datetime.date.fromisoformat(date_str.replace("+", "-"))


def _today_as_bids_date() -> str:
    """Return today's date in BIDS format (YYYY+MM+DD)."""
    return datetime.date.today().isoformat().replace("-", "+")


def _has_dates_within_n_days(dates: set, n_days: int) -> bool:
    """Return True if any date in the set is within the past n_days calendar days (including today)."""
    today = datetime.date.today()
    return any((today - _bids_date_to_python_date(date_str)).days < n_days for date_str in dates)


def _has_output_for_date(asset_paths: list, date_str: str) -> bool:
    """Return True if any asset path for the given date includes an 'output' subdirectory."""
    for asset_path in asset_paths:
        if "/output/" not in asset_path:
            continue
        match = _DATE_PATTERN.search(asset_path)
        if match and match.group(1) == date_str:
            return True
    return False


def check_and_trigger() -> None:
    """
    Scan the testing dandiset for recent outputs.

    If no testing outputs exist within the past 3 days, trigger a new test run,
    sleep for one hour, then verify that today's run produced an 'output' subdirectory.
    """
    dandiset = _get_dandiset()
    asset_paths = _get_testing_asset_paths(dandiset)
    dates = _get_all_testing_dates(asset_paths)

    if not _has_dates_within_n_days(dates, 3):
        script_file_path = prepare_aind_ephys_job(content_id=TESTING_CONTENT_ID, silent=True)
        submit_aind_ephys_job(script_file_path=script_file_path)
        time.sleep(3600)

    # Re-query after potential submission and sleep to check for fresh results
    dandiset = _get_dandiset()
    asset_paths = _get_testing_asset_paths(dandiset)
    today = _today_as_bids_date()

    if not _has_output_for_date(asset_paths, today):
        message = f"No 'output' subdirectory found for today's testing run (date: {today})."
        raise RuntimeError(message)


def check_last_week() -> None:
    """
    Scan the testing dandiset and fail if no testing assets have run in the past week.
    """
    dandiset = _get_dandiset()
    asset_paths = _get_testing_asset_paths(dandiset)
    dates = _get_all_testing_dates(asset_paths)

    if not _has_dates_within_n_days(dates, 7):
        message = "No testing assets have run in the past week."
        raise RuntimeError(message)
