import attr
from attr.validators import instance_of, optional
import time
from typing import Optional, Dict, List, Any

from .mypy_types import Headers, OptBool, OptInt, OptStr, OptTs


# possible span kinds
CLIENT = 'CLIENT'
SERVER = 'SERVER'
PRODUCER = 'PRODUCER'
CONSUMER = 'CONSUMER'

# zipkin headers, for more information see:
# https://github.com/openzipkin/b3-propagation

TRACE_ID_HEADER = 'X-B3-TraceId'
SPAN_ID_HEADER = 'X-B3-SpanId'
PARENT_ID_HEADER = 'X-B3-ParentSpanId'
FLAGS_HEADER = 'X-B3-Flags'
SAMPLED_ID_HEADER = 'X-B3-Sampled'


@attr.s(frozen=True, cmp=True)
class TraceContext:
    """Immutable class with trace related data that travels across
    process boundaries.
    """
    trace_id = attr.ib(type=str)
    span_id = attr.ib(type=str)
    sampled = attr.ib(type=bool)
    debug = attr.ib(type=bool)
    shared = attr.ib(type=bool)
    parent_id = attr.ib(type=str, default=None)

    def make_headers(self) -> Headers:
        """Creates dict with zipkin headers from available context.

        Resulting dict should be passed to HTTP client  propagate contest
        to other services.
        """
        return make_headers(self)

    def _replace(self, **kargs):
        dump_dict = attr.asdict(self)
        dump_dict.update(kargs)
        return TraceContext(**dump_dict)


def _filter_None(attribute, value):
    return value is not None


@attr.s
class Endpoint:
    serviceName = attr.ib()
    ipv4 = attr.ib(type=str, default=None)
    ipv6 = attr.ib(type=str, default=None)
    port = attr.ib(type=int, default=None)

    def to_dict(self):
        return attr.asdict(self, filter=_filter_None)



def create_endpoint(servce_name: str, *,
                    ipv4: OptStr=None,
                    ipv6: OptStr=None,
                    port: OptInt=None):
    """Factory function to create Endpoint object.
    """
    return Endpoint(
                serviceName=servce_name,
                ipv4=ipv4,
                ipv6=ipv6,
                port=port,
            )


def make_timestamp(ts: OptTs=None) -> int:
    """Create zipkin timestamp in microseconds, or convert available one
    from second. Useful when user supply ts from time.time() call.
    """
    ts = ts if ts is not None else time.time()
    return int(ts * 1000 * 1000)  # microseconds


def make_headers(context: TraceContext) -> Headers:
    """Creates dict with zipkin headers from supplied trace context.
    """
    headers = {
        TRACE_ID_HEADER: context.trace_id,
        SPAN_ID_HEADER: context.span_id,
        FLAGS_HEADER: '0',
        SAMPLED_ID_HEADER: '1' if context.sampled else '0',
    }
    if context.parent_id is not None:
        headers[PARENT_ID_HEADER] = context.parent_id
    return headers


def parse_sampled(headers: Headers) -> OptBool:
    sampled = headers.get(SAMPLED_ID_HEADER.lower(), None)
    if sampled is None or sampled == '':
        return None
    # TODO: add more test cases
    return True if sampled == '1' else False


def parse_debug(headers: Headers) -> bool:
    return True if headers.get(FLAGS_HEADER, '0') == '1' else False


def make_context(headers: Headers) -> Optional[TraceContext]:
    """Converts available headers to TraceContext, if headers mapping does
    not contain zipkin headers, function returns None.
    """
    # TODO: add validation for trace_id/span_id/parent_id

    # normalize header names just in case someone passed regular dict
    # instead dict with case insensitive keys
    headers = {k.lower(): v for k, v in headers.items()}

    if not all(h in headers for h in (
            TRACE_ID_HEADER.lower(), SPAN_ID_HEADER.lower())):
        return None

    context = TraceContext(
        trace_id=headers.get(TRACE_ID_HEADER.lower()),
        parent_id=headers.get(PARENT_ID_HEADER.lower(), None),
        span_id=headers.get(SPAN_ID_HEADER.lower()),
        sampled=parse_sampled(headers),
        shared=False,
        debug=parse_debug(headers),
    )
    return context


OptKeys = Optional[List[str]]
