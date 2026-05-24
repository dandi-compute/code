# ruff: noqa: F821
from . import _process_queue_test_cases as _support

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)


@pytest.mark.ai_generated
def test_version_matches_exact() -> None:
    """_version_matches returns True for exact match."""
    assert _version_matches("v1.1.1+b268fd2", "v1.1.1+b268fd2") is True


@pytest.mark.ai_generated
def test_version_matches_with_code_hash_suffix() -> None:
    """_version_matches returns True when state version has an extra code-repo commit hash suffix."""
    assert _version_matches("v1.1.1+b268fd2+abcdef1", "v1.1.1+b268fd2") is True


@pytest.mark.ai_generated
def test_version_matches_rejects_non_hex_suffix() -> None:
    """_version_matches returns False when the extra suffix is not a hex hash."""
    assert _version_matches("v1.1.1+b268fd2+notahex", "v1.1.1+b268fd2") is False


@pytest.mark.ai_generated
def test_version_matches_rejects_different_version() -> None:
    """_version_matches returns False when the version base is different."""
    assert _version_matches("v1.1.0+b268fd2", "v1.1.1+b268fd2") is False
