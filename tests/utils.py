import pytest

slow = pytest.mark.skipif(
    not pytest.config.getoption("--slow"),
    reason="need --slow option to run"
)
