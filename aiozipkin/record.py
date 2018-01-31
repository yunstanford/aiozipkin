import attr
from typing import TypeVar, Dict, Any, List
from .mypy_types import OptInt, OptStr, Optional  # flake8: noqa

from .helpers import (
    CONSUMER,
    PRODUCER,
    Endpoint,
    TraceContext,
)


@attr.s
class Annotation:
    value = attr.ib()
    timestamp = attr.ib()


T = TypeVar('T', bound='Record')


class Record:

    def __init__(self: T, context: TraceContext,
                 local_endpoint: Endpoint) -> None:
        self._context = context
        self._local_endpoint = local_endpoint.to_dict()
        self._finished = False

        self._name = 'unknown'
        self._kind = None  # type: OptStr
        self._timestamp = None  # type: OptInt
        self._duration = None  # type: OptInt
        self._remote_endpoint = None  # type: Optional[Dict[str, Any]]
        self._annotations = []  # type: List[Annotation]
        self._tags = {}  # type: Dict[str, str]

    def start(self: T, ts: int) -> T:
        self._timestamp = ts
        return self

    def finish(self: T, ts: int) -> T:
        if self._finished:
            return self
        if ts is not None and self._kind not in (PRODUCER, CONSUMER):
            self._duration = max(ts - self._timestamp, 1)
        self._finished = True
        return self

    def name(self: T, n: str) -> T:
        self._name = n
        return self

    def set_tag(self: T, key: str, value: Any) -> T:
        self._tags[key] = str(value)
        return self

    def annotate(self: T, value: str, ts: int) -> T:
        self._annotations.append(Annotation(value=str(value), timestamp=int(ts)))
        return self

    def kind(self: T, kind: str) -> T:
        self._kind = kind
        return self

    def remote_endpoint(self: T, endpoint: Endpoint) -> T:
        self._remote_endpoint = endpoint.to_dict()
        return self

    def asdict(self) -> Dict[str, Any]:
        c = self._context
        rec = {
            'traceId': c.trace_id,
            'name': self._name,
            'parentId': c.parent_id,
            'id': c.span_id,
            'kind': self._kind,
            'timestamp': self._timestamp,
            'duration': self._duration,
            'debug': c.debug,
            'shared': c.shared,
            'localEndpoint': self._local_endpoint,
            'remoteEndpoint': self._remote_endpoint,
            'annotations': [attr.asdict(a) for a in self._annotations],
            'tags': self._tags,
        }
        return filter_none(rec, ['kind'])
