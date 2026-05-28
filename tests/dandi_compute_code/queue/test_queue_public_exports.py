from dandi_compute_code import queue
from dandi_compute_code.queue import JobInfo


def test_queue_exports_job_info() -> None:
    assert queue.JobInfo is JobInfo
