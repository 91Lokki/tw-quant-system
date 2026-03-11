"""Paper execution placeholder.

This module exists so the repository already has an execution boundary, but it does
not implement any paper-trading logic yet.
"""


def describe_paper_execution_scope() -> str:
    """Describe the future responsibility of the paper-execution module."""

    return (
        "Future paper execution will consume portfolio outputs, simulate order handling, "
        "and record fills without touching a live broker."
    )
