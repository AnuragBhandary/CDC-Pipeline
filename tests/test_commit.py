from __future__ import annotations

import pytest
from confluent_kafka import KafkaError, KafkaException

from cdcpipe.sink import Stats, commit_offsets


class FakeConsumer:
    def __init__(self, code: int | None) -> None:
        self.code = code

    def commit(self, asynchronous: bool) -> None:
        if self.code is not None:
            raise KafkaException(KafkaError(self.code))


def test_commit_ok() -> None:
    st = Stats("r", "c")
    assert commit_offsets(FakeConsumer(None), st) and st.commit_failures == 0


@pytest.mark.parametrize(
    "code",
    [KafkaError.ILLEGAL_GENERATION, KafkaError.REBALANCE_IN_PROGRESS, KafkaError.UNKNOWN_MEMBER_ID],
)
def test_commit_lost_to_rebalance_is_tolerated(code: int) -> None:
    st = Stats("r", "c")
    assert not commit_offsets(FakeConsumer(code), st)
    assert st.commit_failures == 1


def test_other_commit_errors_are_fatal() -> None:
    with pytest.raises(KafkaException):
        commit_offsets(FakeConsumer(KafkaError.FENCED_INSTANCE_ID), Stats("r", "c"))
