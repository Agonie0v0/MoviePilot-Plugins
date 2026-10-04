"""MoviePilot V3 compatibility tests (run separately from the V2 harness)."""


def load_tests(loader, tests, pattern):
    """Keep V2 unittest discovery independent of V3's pytest dependencies."""
    return tests
