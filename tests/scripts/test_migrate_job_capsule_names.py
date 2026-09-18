"""
Tests for the one-off legacy job capsule migration script.

The script works against local Dandiset clones, so these build a small clone in a temporary
directory and drive the phases over it. Only ``upload`` and ``clean`` touch the network, and
their ``dandi`` invocations are mocked.

The script is standalone by design, so it is loaded by path rather than imported as a module
of the package.
"""

import ast
import contextlib
import datetime
import importlib.util
import io
import json
import os
import pathlib
import shutil
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


def _fake_urlopen(payload: dict):
    """Stand in for urlopen, serving *payload* as the archive's assets.jsonld."""
    body = json.dumps(payload).encode("utf-8")

    @contextlib.contextmanager
    def opener(url):  # noqa: ARG001 - the URL is irrelevant to the stub
        yield io.BytesIO(body)

    return opener


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
def test_plan_dates_the_job_id_from_the_submission_marker(tmp_path: pathlib.Path) -> None:
    """
    The job ID carries the date the capsule's job was submitted.

    That date lives in the marker's file name, so it survives the capsule being re-uploaded,
    which the modification times do not.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    code_dir = root / _DANDISET_ID / _AIND_PIPELINE_PATH / _LEGACY_FLAT_NAME / "code"
    (code_dir / "submitted_date-2024+11+03_time-14+32+09").write_bytes(b"1")

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert plan[0]["job_id"].startswith("job-241103")


@pytest.mark.ai_generated
def test_plan_dates_a_resubmitted_capsule_from_its_first_submission(tmp_path: pathlib.Path) -> None:
    """A capsule submitted more than once keeps the date of its first run."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    code_dir = root / _DANDISET_ID / _AIND_PIPELINE_PATH / _LEGACY_FLAT_NAME / "code"
    (code_dir / "submitted_date-2025+02+18_time-09+00+00").write_bytes(b"1")
    (code_dir / "submitted_date-2024+11+03_time-14+32+09").write_bytes(b"1")

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert plan[0]["job_id"].startswith("job-241103")


@pytest.mark.ai_generated
def test_plan_falls_back_to_the_submission_script_when_never_submitted(tmp_path: pathlib.Path) -> None:
    """A capsule with no submission marker is dated from when its files last landed."""
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
def test_plan_indexes_capsules_that_would_share_a_job_id(tmp_path: pathlib.Path) -> None:
    """
    Two capsules differing only in codebase version are the same logical job prepared on the
    same day, so they map to one job ID. Copying both onto one directory would merge them, so
    the second is suffixed with a counter and both migrate.
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

    job_ids = sorted(record["job_id"] for record in plan)
    assert len(job_ids) == 2
    assert job_ids[1] == f"{job_ids[0]}-2"
    # Each capsule keeps a directory of its own, and the provenance agrees with the name.
    assert len({record["new_path"] for record in plan}) == 2
    assert all(record["new_path"].endswith(record["identity"]["job_id"]) for record in plan)


@pytest.mark.ai_generated
def test_plan_assigns_the_counter_by_legacy_path(tmp_path: pathlib.Path) -> None:
    """The same clone always produces the same names, whatever order the capsules are found."""
    script = _load_script()
    capsule_names = [
        "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678",
        "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678",
    ]
    root = _clone(tmp_path, capsule_names)

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    by_old_path = {record["old_path"]: record["job_id"] for record in plan}
    first, second = (f"{_AIND_PIPELINE_PATH}/{name}" for name in sorted(capsule_names))
    assert not by_old_path[first].endswith("-2")
    assert by_old_path[second] == f"{by_old_path[first]}-2"


@pytest.mark.ai_generated
def test_plan_leaves_an_indexed_capsule_alone_once_migrated(tmp_path: pathlib.Path) -> None:
    """A capsule already carrying a counter reads as migrated, not as something to migrate."""
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123-2", _LEGACY_FLAT_NAME])

    plan = script.plan_migration(dandiset_root=root / _DANDISET_ID)

    assert len(plan) == 1
    assert plan[0]["old_path"].endswith(_LEGACY_FLAT_NAME)


@pytest.mark.ai_generated
def test_copy_duplicates_capsules_and_writes_provenance(tmp_path: pathlib.Path) -> None:
    """The copy phase is purely local: it duplicates on disk and records the provenance."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME], with_output=True)
    dandiset_root = root / _DANDISET_ID

    plan = script.plan_migration(dandiset_root=dandiset_root)
    copied = script.copy_capsules(dandiset_root=dandiset_root, plan=plan)

    assert len(copied) == 1
    new_dir = dandiset_root / copied[0]["new_path"]
    assert new_dir.is_dir()
    # The legacy path survives: the archive deletes it only in the clean phase.
    assert (dandiset_root / copied[0]["old_path"]).is_dir()
    # The whole capsule was copied, not just its code directory.
    assert (new_dir / "derivatives" / "output.nwb").read_text() == "output\n"

    provenance = json.loads((new_dir / "dataset_description.json").read_text())["DandiCompute"]
    assert provenance["job_id"] == copied[0]["job_id"]
    assert provenance["version"] == "v1.1.0"
    assert provenance["codebase"] == "v0.3.0"
    assert provenance["params"] == "abc1234"
    assert provenance["config"] == "def5678"
    assert provenance["content_id"] == _SOURCE_CONTENT_ID


@pytest.mark.ai_generated
def test_copy_leaves_the_legacy_version_directory_in_place(tmp_path: pathlib.Path) -> None:
    """
    The nested layout's `version-` parent stays until clean.

    The legacy capsule under it is still on the archive at this point, so the clone has to keep
    mirroring it.
    """
    script = _load_script()
    legacy_name = "version-v1.0.0/params-abc1234_config-def5678"
    root = _clone(tmp_path, [legacy_name])
    dandiset_root = root / _DANDISET_ID

    plan = script.plan_migration(dandiset_root=dandiset_root)
    script.copy_capsules(dandiset_root=dandiset_root, plan=plan)

    assert (dandiset_root / _AIND_PIPELINE_PATH / legacy_name).is_dir()


@pytest.mark.ai_generated
def test_copy_is_idempotent(tmp_path: pathlib.Path) -> None:
    """
    Re-running the copy phase makes no second copy.

    The legacy capsule is still on disk, so it is planned again. The copy itself is what
    recognises the existing job ID directory, and it re-reports the capsule rather than
    skipping it, so a manifest that was lost is rebuilt by re-running.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_root = root / _DANDISET_ID
    pipeline_dir = dandiset_root / _AIND_PIPELINE_PATH

    first_copied = script.copy_capsules(
        dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root)
    )
    directories_after_first = sorted(child.name for child in pipeline_dir.iterdir())
    second_copied = script.copy_capsules(
        dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root)
    )

    assert [record["new_path"] for record in second_copied] == [record["new_path"] for record in first_copied]
    assert sorted(child.name for child in pipeline_dir.iterdir()) == directories_after_first


@pytest.mark.ai_generated
def test_copy_keeps_a_capsule_on_its_own_copy_when_the_group_shrinks(tmp_path: pathlib.Path) -> None:
    """
    A capsule keeps the copy it already has, whatever counter that copy took.

    The counter is assigned by position when the plan is built, so removing a capsule from a
    group shifts the positions of those left. Trusting the position would hand the second
    capsule the directory holding the first one's copy, record it as migrated, and let clean
    delete its legacy path though its contents were never uploaded.
    """
    script = _load_script()
    first_name = "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678"
    second_name = "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678"
    root = _clone(tmp_path, [first_name, second_name])
    dandiset_root = root / _DANDISET_ID
    pipeline_dir = dandiset_root / _AIND_PIPELINE_PATH

    first_copied = script.copy_capsules(
        dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root)
    )
    second_job_id = next(record["job_id"] for record in first_copied if record["old_path"].endswith(second_name))

    # The first capsule is taken out of the group, which shifts the second one's position.
    shutil.rmtree(pipeline_dir / first_name)
    re_copied = script.copy_capsules(
        dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root)
    )

    assert [record["job_id"] for record in re_copied] == [second_job_id]
    assert (pipeline_dir / second_job_id / "dataset_description.json").is_file()
    provenance = json.loads((pipeline_dir / second_job_id / "dataset_description.json").read_text())["DandiCompute"]
    assert provenance["migrated_from"] == f"{_AIND_PIPELINE_PATH}/{second_name}"


@pytest.mark.ai_generated
def test_copy_records_where_each_capsule_came_from(tmp_path: pathlib.Path) -> None:
    """The provenance names the legacy path, which is what makes a copy attributable."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_root = root / _DANDISET_ID

    copied = script.copy_capsules(dandiset_root=dandiset_root, plan=script.plan_migration(dandiset_root=dandiset_root))

    provenance = json.loads((dandiset_root / copied[0]["new_path"] / "dataset_description.json").read_text())
    assert provenance["DandiCompute"]["migrated_from"] == f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}"


@pytest.mark.ai_generated
def test_copy_re_run_keeps_the_earlier_runs_manifest_records(tmp_path: pathlib.Path) -> None:
    """
    A second copy run adds to the manifest instead of replacing it.

    Writing just its own records would drop the earlier run's, stranding capsules that are
    copied on disk but unknown to the later phases.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_ids = [_DANDISET_ID]

    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    first_records = json.loads(script.manifest_path(root).read_text())["dandisets"][_DANDISET_ID]

    _make_capsule(dandiset_root=root / _DANDISET_ID, capsule_name="version-v2.0.0_codebase-v0.3.0_params-999aaaa")
    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    second_records = json.loads(script.manifest_path(root).read_text())["dandisets"][_DANDISET_ID]

    assert len(first_records) == 1
    assert len(second_records) == 2
    assert first_records[0] in second_records


@pytest.mark.ai_generated
def test_copy_re_run_records_each_capsule_once(tmp_path: pathlib.Path) -> None:
    """Re-running with nothing new to do leaves the manifest as it was."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_ids = [_DANDISET_ID]

    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    first_manifest = json.loads(script.manifest_path(root).read_text())["dandisets"]
    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    second_manifest = json.loads(script.manifest_path(root).read_text())["dandisets"]

    assert second_manifest == first_manifest


@pytest.mark.ai_generated
def test_upload_batches_only_the_new_paths(tmp_path: pathlib.Path) -> None:
    """The upload phase pushes the copied paths and deletes nothing."""
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
    """copy writes a manifest that upload and clean read back, so the phases chain up."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_ids = [_DANDISET_ID]

    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    manifest = json.loads(script.manifest_path(root).read_text())
    assert len(manifest["dandisets"][_DANDISET_ID]) == 1
    record = manifest["dandisets"][_DANDISET_ID][0]

    with mock.patch.object(script, "_run") as mock_run:
        script._phase_upload(root=root, dandiset_ids=dandiset_ids)
    assert mock_run.call_args.args[0] == ["dandi", "upload", "--allow-any-path", record["new_path"]]

    with mock.patch.object(script, "_run") as mock_run:
        script._phase_clean(root=root, dandiset_ids=dandiset_ids, reconcile=False)
    assert mock_run.call_args.args[0] == [
        "dandi",
        "delete",
        f"dandi://dandi/{_DANDISET_ID}/{record['old_path']}/",
    ]


@pytest.mark.ai_generated
def test_clean_removes_the_legacy_copy_left_behind_locally(tmp_path: pathlib.Path) -> None:
    """The legacy directory the copy phase preserved is torn down once clean has run."""
    script = _load_script()
    root = _clone(tmp_path, ["version-v1.0.0/params-abc1234_config-def5678"])
    dandiset_ids = [_DANDISET_ID]
    dandiset_root = root / _DANDISET_ID

    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    assert (dandiset_root / _AIND_PIPELINE_PATH / "version-v1.0.0").is_dir()

    with mock.patch.object(script, "_run"):
        script._phase_clean(root=root, dandiset_ids=dandiset_ids, reconcile=False)

    assert not (dandiset_root / _AIND_PIPELINE_PATH / "version-v1.0.0").exists()
    assert (dandiset_root / _AIND_PIPELINE_PATH).is_dir()


def _archive_assets(paths: list[str]) -> dict:
    """Shape a list of asset paths the way the archive's assets.jsonld presents them."""
    return {"hasPart": [{"path": path} for path in paths]}


@pytest.mark.ai_generated
def test_reconcile_pairs_an_orphaned_legacy_path_with_its_migrated_capsule(tmp_path: pathlib.Path) -> None:
    """
    A capsule an earlier run moved rather than copied is recoverable from the archive.

    Its legacy directory is gone locally, so the plan phase cannot see it, but the archive
    still carries the legacy path next to the uploaded job ID capsule.
    """
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    dandiset_root = root / _DANDISET_ID
    (dandiset_root / _AIND_PIPELINE_PATH / "job-250607abc123" / "dataset_description.json").write_text(
        json.dumps(
            {
                "DandiCompute": {
                    "version": "v1.1.0",
                    "codebase": "v0.3.0",
                    "params": "abc1234",
                    "config": "def5678",
                }
            }
        )
    )
    archive_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(archive_paths))):
        orphans = script.find_orphaned_legacy_paths(dandiset_root=dandiset_root, dandiset_id=_DANDISET_ID)

    assert len(orphans) == 1
    assert orphans[0]["old_path"] == f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}"
    assert orphans[0]["new_path"] == f"{_AIND_PIPELINE_PATH}/job-250607abc123"


@pytest.mark.ai_generated
def test_reconcile_records_every_attempt_sharing_one_migrated_capsule(tmp_path: pathlib.Path) -> None:
    """
    Re-attempts of one job differ only by a suffix the identity ignores, so they all pair with
    the same migrated capsule. Each is still its own path on the archive and must be recorded.
    """
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    dandiset_root = root / _DANDISET_ID
    (dandiset_root / _AIND_PIPELINE_PATH / "job-250607abc123" / "dataset_description.json").write_text(
        json.dumps(
            {"DandiCompute": {"version": "v1.1.0", "codebase": "v0.3.0", "params": "abc1234", "config": "def5678"}}
        )
    )
    archive_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}_attempt-1/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}_attempt-2/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(archive_paths))):
        script._phase_reconcile(root=root, dandiset_ids=[_DANDISET_ID])

    records = json.loads(script.manifest_path(root).read_text())["dandisets"][_DANDISET_ID]
    assert sorted(record["old_path"] for record in records) == [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}_attempt-1",
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}_attempt-2",
    ]


@pytest.mark.ai_generated
def test_reconcile_leaves_a_legacy_path_with_no_migrated_capsule_alone(tmp_path: pathlib.Path) -> None:
    """
    A legacy path is only recorded once its replacement is confirmed on the archive.

    Recording an unpaired one would let clean delete a capsule that was never migrated.
    """
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    dandiset_root = root / _DANDISET_ID
    (dandiset_root / _AIND_PIPELINE_PATH / "job-250607abc123" / "dataset_description.json").write_text(
        json.dumps({"DandiCompute": {"version": "v9.9.9", "codebase": "v0.3.0", "params": "zzz", "config": "yyy"}})
    )
    archive_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(archive_paths))):
        orphans = script.find_orphaned_legacy_paths(dandiset_root=dandiset_root, dandiset_id=_DANDISET_ID)

    assert orphans == []


@pytest.mark.ai_generated
def test_reconcile_writes_a_manifest_that_clean_deletes_from(tmp_path: pathlib.Path) -> None:
    """The reconcile phase hands off to clean through the same manifest the copy phase uses."""
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    dandiset_root = root / _DANDISET_ID
    (dandiset_root / _AIND_PIPELINE_PATH / "job-250607abc123" / "dataset_description.json").write_text(
        json.dumps(
            {
                "DandiCompute": {
                    "version": "v1.1.0",
                    "codebase": "v0.3.0",
                    "params": "abc1234",
                    "config": "def5678",
                }
            }
        )
    )
    archive_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(archive_paths))):
        script._phase_reconcile(root=root, dandiset_ids=[_DANDISET_ID])

    with mock.patch.object(script, "_run") as mock_run:
        script._phase_clean(root=root, dandiset_ids=[_DANDISET_ID], reconcile=False)

    assert mock_run.call_args.args[0] == [
        "dandi",
        "delete",
        f"dandi://dandi/{_DANDISET_ID}/{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/",
    ]


@pytest.mark.ai_generated
def test_clean_also_deletes_orphans_the_manifest_never_recorded(tmp_path: pathlib.Path) -> None:
    """
    clean reconciles against the archive, so improperly named folders left by an earlier
    migration are removed alongside the capsules this run copied.
    """
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    dandiset_root = root / _DANDISET_ID
    (dandiset_root / _AIND_PIPELINE_PATH / "job-250607abc123" / "dataset_description.json").write_text(
        json.dumps(
            {"DandiCompute": {"version": "v1.1.0", "codebase": "v0.3.0", "params": "abc1234", "config": "def5678"}}
        )
    )
    script.manifest_path(root).write_text(json.dumps({"dandisets": {_DANDISET_ID: []}}) + "\n")
    archive_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(archive_paths))):
        with mock.patch.object(script, "_run") as mock_run:
            script._phase_clean(root=root, dandiset_ids=[_DANDISET_ID])

    assert mock_run.call_args.args[0] == [
        "dandi",
        "delete",
        f"dandi://dandi/{_DANDISET_ID}/{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/",
    ]


@pytest.mark.ai_generated
def test_clean_is_idempotent(tmp_path: pathlib.Path) -> None:
    """
    Re-running clean deletes nothing a second time.

    The manifest still records the migrated capsules, but the archive no longer holds their
    legacy paths, and asking it to delete a path that is gone would fail the phase.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME])
    dandiset_ids = [_DANDISET_ID]
    before_paths = [
        f"{_AIND_PIPELINE_PATH}/{_LEGACY_FLAT_NAME}/code/submit.sh",
        f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh",
    ]

    script._phase_copy(root=root, dandiset_ids=dandiset_ids)
    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(before_paths))):
        with mock.patch.object(script, "_run") as first_run:
            script._phase_clean(root=root, dandiset_ids=dandiset_ids)

    # The archive no longer lists the legacy path once the first clean has deleted it.
    after_paths = [f"{_AIND_PIPELINE_PATH}/job-250607abc123/code/submit.sh"]
    with mock.patch.object(script.urllib.request, "urlopen", _fake_urlopen(_archive_assets(after_paths))):
        with mock.patch.object(script, "_run") as second_run:
            script._phase_clean(root=root, dandiset_ids=dandiset_ids)

    assert first_run.call_count == 1
    second_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_without_reconcile_deletes_only_what_the_manifest_records(tmp_path: pathlib.Path) -> None:
    """`--no-reconcile` keeps clean off the archive listing, deleting only recorded paths."""
    script = _load_script()
    root = _clone(tmp_path, ["job-250607abc123"])
    script.manifest_path(root).write_text(json.dumps({"dandisets": {_DANDISET_ID: []}}) + "\n")

    with mock.patch.object(script.urllib.request, "urlopen") as mock_urlopen:
        with mock.patch.object(script, "_run") as mock_run:
            script._phase_clean(root=root, dandiset_ids=[_DANDISET_ID], reconcile=False)

    mock_urlopen.assert_not_called()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize("phase", ["upload", "clean"])
def test_phases_requiring_the_archive_fail_without_a_manifest(tmp_path: pathlib.Path, phase: str) -> None:
    """upload and clean refuse to guess: they need the manifest the copy phase wrote."""
    script = _load_script()

    with pytest.raises(RuntimeError, match="No migration manifest"):
        script._PHASES[phase](root=tmp_path, dandiset_ids=[_DANDISET_ID])


@pytest.mark.ai_generated
def test_scan_does_not_descend_into_capsule_contents(tmp_path: pathlib.Path) -> None:
    """
    The scan stops at each `pipeline-*` directory.

    A capsule's own output tree can hold the bulk of a clone, and walking it dominates the
    scan on a slow mount. A stray `code` directory inside a capsule's output must therefore
    neither be visited nor mistaken for a capsule of its own.
    """
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME], with_output=True)
    dandiset_root = root / _DANDISET_ID
    capsule_dir = dandiset_root / _AIND_PIPELINE_PATH / _LEGACY_FLAT_NAME
    decoy = capsule_dir / "derivatives" / "nested" / "code"
    decoy.mkdir(parents=True)

    capsules = script.find_legacy_capsules(dandiset_root)

    assert capsules == [capsule_dir]


@pytest.mark.ai_generated
def test_scan_visits_far_fewer_entries_than_walking_everything(tmp_path: pathlib.Path) -> None:
    """The pruned scan is bounded by the tree's shape, not by how much output the capsules hold."""
    script = _load_script()
    root = _clone(tmp_path, [_LEGACY_FLAT_NAME], with_output=True)
    dandiset_root = root / _DANDISET_ID
    output_dir = dandiset_root / _AIND_PIPELINE_PATH / _LEGACY_FLAT_NAME / "derivatives"
    for index in range(200):
        (output_dir / f"part-{index:04d}.dat").write_text("x")

    visited: list[pathlib.Path] = []
    real_scandir = os.scandir

    def _counting_scandir(path):
        visited.append(pathlib.Path(path))
        return real_scandir(path)

    with mock.patch.object(os, "scandir", _counting_scandir):
        script.find_legacy_capsules(dandiset_root)

    assert not any(str(path).endswith("derivatives") and "pipeline-" in str(path) for path in visited)
    assert len(visited) < 20
