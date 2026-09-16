"""
Tests for the one-off legacy job capsule migration script.

The script works against local Dandiset clones, so these build a small clone in a temporary
directory and drive the phases over it. Only ``upload`` and ``clean`` touch the network, and
their ``dandi`` invocations are mocked.

The script is standalone by design, so it is loaded by path rather than imported as a module
of the package.
"""

import ast
import datetime
import importlib.util
import json
import os
import pathlib
import sys
from unittest import mock

import pytest

from dandi_compute_code.dandiset._job_id import _compute_job_hash

_SCRIPT_PATH = pathlib.Path(__file__).parent.parent.parent / "scripts" / "migrate_job_capsule_names.py"

_DANDISET_ID = "001697"
_SOURCE_CONTENT_ID = "0fbbca6a-1111-2222-3333-444444444444"
_PREPARED_ON = datetime.date(2025, 6, 7)
_AIND_PIPELINE_PATH = "derivatives/dandisets-000/dandiset-000409/sub-01/sub-01_ecephys/pipeline-aind+ephys"
_LEGACY_FLAT_NAME = "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678"


def _load_script():
    """Load the standalone script by path, since ``scripts/`` is not an installed package."""
    spec = importlib.util.spec_from_file_location("migrate_job_capsule_names", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _make_capsule(
    *,
    dandiset_root: pathlib.Path,
    capsule_name: str,
    pipeline_path: str = _AIND_PIPELINE_PATH,
    content_id: str = _SOURCE_CONTENT_ID,
    with_output: bool = False,
) -> pathlib.Path:
    """Materialize one capsule directory inside a local clone."""
    capsule_dir = dandiset_root / pipeline_path / capsule_name
    code_dir = capsule_dir / "code"
    code_dir.mkdir(parents=True)

    submission_script = code_dir / "submit.sh"
    submission_script.write_text(f'#!/bin/bash\nNWB_FILE_PATH="/blobs/0fb/bca/{content_id}"\necho hello\n')
    (capsule_dir / "dataset_description.json").write_text(json.dumps({"Name": "example"}) + "\n")
    if with_output:
        (capsule_dir / "derivatives").mkdir()
        (capsule_dir / "derivatives" / "output.nwb").write_text("output\n")

    prepared_at = datetime.datetime.combine(_PREPARED_ON, datetime.time(12, 0), tzinfo=datetime.timezone.utc)
    os.utime(submission_script, (prepared_at.timestamp(), prepared_at.timestamp()))
    return capsule_dir


def _clone(tmp_path: pathlib.Path, capsule_names: list[str], **capsule_kwargs) -> pathlib.Path:
    """Build a local clone root holding one Dandiset directory with the given capsules."""
    dandiset_root = tmp_path / _DANDISET_ID
    for capsule_name in capsule_names:
        _make_capsule(dandiset_root=dandiset_root, capsule_name=capsule_name, **capsule_kwargs)
    return tmp_path


@pytest.mark.ai_generated
def test_script_imports_nothing_from_the_package() -> None:
    """
    The script must stay standalone so it runs against any installed version of the package,
    including one that predates the job ID.
    """
    tree = ast.parse(_SCRIPT_PATH.read_text())
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module.split(".")[0])

    assert "dandi_compute_code" not in imported_roots
    assert imported_roots <= set(sys.stdlib_module_names)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("capsule_name", "expected_version", "expected_codebase", "expected_config"),
    [
        pytest.param(_LEGACY_FLAT_NAME, "v1.1.0", "v0.3.0", "def5678", id="flat"),
        pytest.param(
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-2",
            "v1.1.0",
            "v0.3.0",
            "def5678",
            id="flat_attempt",
        ),
        pytest.param("version-v1.0.0/params-abc1234_config-def5678", "v1.0.0", "", "def5678", id="nested"),
        pytest.param(
            "version-v1.0.0/params-abc1234_config-def5678_attempt-3", "v1.0.0", "", "def5678", id="nested_attempt"
        ),
        pytest.param("version-v1.1.0_codebase-v0.3.0_params-abc1234", "v1.1.0", "v0.3.0", "", id="flat_no_config"),
    ],
)
def test_plan_parses_every_legacy_layout(
    tmp_path: pathlib.Path,
    capsule_name: str,
    expected_version: str,
    expected_codebase: str,
    expected_config: str,
) -> None:
    """Each legacy capsule layout is parsed back into the identity its name spelled out."""
    script = _load_script()
    root = _clone(tmp_path, [capsule_name])

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert len(plan) == 1
    record = plan[0]
    assert record["old_path"] == f"{_AIND_PIPELINE_PATH}/{capsule_name}"
    assert record["new_path"] == f"{_AIND_PIPELINE_PATH}/{record['job_id']}"
    assert record["identity"]["version"] == expected_version
    assert record["identity"]["codebase"] == expected_codebase
    assert record["identity"]["config"] == expected_config
    assert record["identity"]["params"] == "abc1234"
    assert record["identity"]["dandi_path"] == "sub-01/sub-01_ecephys.nwb"
    assert record["identity"]["content_id"] == _SOURCE_CONTENT_ID


@pytest.mark.ai_generated
def test_plan_dates_the_job_id_from_the_submission_script(tmp_path: pathlib.Path) -> None:
    """A migrated capsule keeps the date it was originally prepared."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert plan[0]["job_id"].startswith(f"job-{_PREPARED_ON:%y%m%d}")


@pytest.mark.ai_generated
def test_plan_hash_matches_what_preparation_would_compute(tmp_path: pathlib.Path) -> None:
    """
    A migrated job keeps the hash preparation gives it, so it is never formed a second time.

    The script carries its own copy of the hash, so this pins that copy to the package's.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    expected_hash = _compute_job_hash(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        pipeline="aind+ephys",
        version="v1.1.0",
        params="abc1234",
        config="def5678",
        content_id=_SOURCE_CONTENT_ID,
    )
    assert plan[0]["job_id"].removeprefix("job-")[6:] == expected_hash


@pytest.mark.ai_generated
def test_plan_ignores_already_migrated_capsules(tmp_path: pathlib.Path) -> None:
    """Capsules that already carry a job ID are left out of the plan."""
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123", _LEGACY_FLAT_NAME])

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert len(plan) == 1
    assert plan[0]["old_path"].endswith(_LEGACY_FLAT_NAME)


@pytest.mark.ai_generated
def test_plan_skips_capsules_that_would_collide(tmp_path: pathlib.Path) -> None:
    """
    Two capsules differing only in codebase version are the same logical job, so they map to
    one job ID. Renaming both would merge them, so neither is planned.
    """
    script = _load_script()
    root = _clone(
        tmp_path,
        [
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678",
            "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678",
        ],
    )

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert plan == []


@pytest.mark.ai_generated
def test_rename_moves_capsules_and_writes_provenance(tmp_path: pathlib.Path) -> None:
    """The rename phase is purely local: it renames on disk and records the provenance."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME], with_output=True)
    dandiset_root = root / _DANDISET_ID

    plan = script.plan_migration(dandiset_root=dandiset_root)
    renamed = script.rename_capsules(dandiset_root=dandiset_root, plan=plan)

    assert len(renamed) == 1
    new_dir = dandiset_root / renamed[0]["new_path"]
    assert new_dir.is_dir()
    assert not (dandiset_root / renamed[0]["old_path"]).exists()
    # The whole capsule moved, not just its code directory.
    assert (new_dir / "derivatives" / "output.nwb").read_text() == "output\n"

    provenance = json.loads((new_dir / "dataset_description.json").read_text())["DandiCompute"]
    assert provenance["job_id"] == renamed[0]["job_id"]
    assert provenance["version"] == "v1.1.0"
    assert provenance["codebase"] == "v0.3.0"
    assert provenance["params"] == "abc1234"
    assert provenance["config"] == "def5678"
    assert provenance["content_id"] == _SOURCE_CONTENT_ID


@pytest.mark.ai_generated
def test_rename_prunes_the_emptied_legacy_version_directory(tmp_path: pathlib.Path) -> None:
    """The nested layout's now-empty `version-` directory does not linger after the rename."""
    script = _load_script()
    root = _clone(tmp_path, ["version-v1.0.0/params-abc1234_config-def5678"])
    dandiset_root = root / _DANDISET_ID

    plan = script.plan_migration(dandiset_root=dandiset_root)
    script.rename_capsules(dandiset_root=dandiset_root, plan=plan)

    assert not (dandiset_root / _AIND_PIPELINE_PATH / "version-v1.0.0").exists()
    assert (dandiset_root / _AIND_PIPELINE_PATH).is_dir()


@pytest.mark.ai_generated
def test_rename_is_idempotent(tmp_path: pathlib.Path) -> None:
    """Re-running the rename phase finds nothing left to do."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_root = root / _DANDISET_ID

    script.rename_capsules(dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root))
    second_plan = script.plan_migration(dandiset_root=dandiset_root)

    assert second_plan == []


@pytest.mark.ai_generated
def test_upload_batches_only_the_new_paths(tmp_path: pathlib.Path) -> None:
    """The upload phase pushes the renamed paths and deletes nothing."""
    script = _load_script()
    dandiset_root = tmp_path / _DANDISET_ID
    dandiset_root.mkdir()
    new_paths = [f"{_AIND_PIPELINE_PATH}/job-250607{index:06d}" for index in range(3)]

    with mock.patch.object(script, "_run") as mock_run:
        script.upload_new_paths(dandiset_root=dandiset_root, new_paths=new_paths)

    mock_run.assert_called_once_with(["dandi", "upload", "--allow-any-path", *new_paths], cwd=dandiset_root)


@pytest.mark.ai_generated
def test_upload_splits_large_plans_into_batches(tmp_path: pathlib.Path) -> None:
    """A plan larger than one batch is handed to several invocations."""
    script = _load_script()
    dandiset_root = tmp_path / _DANDISET_ID
    dandiset_root.mkdir()
    new_paths = [f"{_AIND_PIPELINE_PATH}/job-250607{index:06d}" for index in range(script._BATCH_SIZE + 1)]

    with mock.patch.object(script, "_run") as mock_run:
        script.upload_new_paths(dandiset_root=dandiset_root, new_paths=new_paths)

    assert mock_run.call_count == 2
    assert len(mock_run.call_args_list[0].args[0]) == script._BATCH_SIZE + 3
    assert len(mock_run.call_args_list[1].args[0]) == 4


@pytest.mark.ai_generated
def test_clean_deletes_the_legacy_paths_by_url() -> None:
    """The clean phase removes the legacy paths from the archive."""
    script = _load_script()
    old_paths = [f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}"]

    with mock.patch.object(script, "_run") as mock_run:
        script.delete_legacy_paths(dandiset_id=_DANDISET_ID, old_paths=old_paths)

    mock_run.assert_called_once_with(
        ["dandi", "delete", f"dandi://dandi/{_DANDISET_ID}/{old_paths[0]}/"],
        input_text="y\n",
    )


@pytest.mark.ai_generated
def test_phases_hand_off_through_the_manifest(tmp_path: pathlib.Path) -> None:
    """rename writes a manifest that upload and clean read back, so the phases chain up."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_ids = [_DANDISET_ID]

    script._phase_rename(root=root, dandiset_ids=dandiset_ids)
    manifest = json.loads(script.manifest_path(root).read_text())
    assert len(manifest["dandisets"][_DANDISET_ID]) == 1
    record = manifest["dandisets"][_DANDISET_ID][0]

    with mock.patch.object(script, "_run") as mock_run:
        script._phase_upload(root=root, dandiset_ids=dandiset_ids)
    assert mock_run.call_args.args[0] == ["dandi", "upload", "--allow-any-path", record["new_path"]]

    with mock.patch.object(script, "_run") as mock_run:
        script._phase_clean(root=root, dandiset_ids=dandiset_ids)
    assert mock_run.call_args.args[0] == [
        "dandi",
        "delete",
        f"dandi://dandi/{_DANDISET_ID}/{record['old_path']}/",
    ]


@pytest.mark.ai_generated
@pytest.mark.parametrize("phase", ["upload", "clean"])
def test_phases_requiring_the_archive_fail_without_a_manifest(tmp_path: pathlib.Path, phase: str) -> None:
    """upload and clean refuse to guess: they need the manifest the rename phase wrote."""
    script = _load_script()

    with pytest.raises(RuntimeError, match="No migration manifest"):
        script._PHASES[phase](root=tmp_path, dandiset_ids=[_DANDISET_ID])
