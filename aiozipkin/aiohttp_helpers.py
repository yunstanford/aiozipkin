import aiohttp
import asyncio
from aiohttp.web import HTTPException

from .constants import HTTP_PATH, HTTP_STATUS_CODE, HTTP_METHOD, HTTP_REDIRECT
from .helpers import make_context, SERVER, parse_debug, parse_sampled


APP_AIOZIPKIN_KEY = 'aiozipkin_tracer'
REQUEST_AIOZIPKIN_KEY = 'aiozipkin_span'


def set_task_ctx(key, value):
    t = asyncio.Task.current_task()
    if hasattr(t, '_context'):
        t._context[key] = value
    else:
        t._context = {key: value}


def get_task_ctx(key):
    t = asyncio.Task.current_task()
    if hasattr(t, '_context'):
        return t._context.get(key)
    return None


def middleware_maker(tracer_key=APP_AIOZIPKIN_KEY,
                     request_key=REQUEST_AIOZIPKIN_KEY):
    async def middleware_factory(app, handler):
        async def aiozipkin_middleware(request):
            context = make_context(request.headers)
            tracer = app[tracer_key]

            if context is None:
                sampled = parse_sampled(request.headers)
                debug = parse_debug(request.headers)
                span = tracer.new_trace(sampled=sampled, debug=debug)
            else:
                span = tracer.join_span(context)

            request[request_key] = span

            if span.is_noop:
                resp = await handler(request)
                return resp

            with span:
                span_name = '{0} {1}'.format(request.method.upper(),
                                             request.path)
                span.name(span_name)
                span.kind(SERVER)
                span.tag(HTTP_PATH, request.path)
                span.tag(HTTP_METHOD, request.method.upper())

                try:
                    resp = await handler(request)
                except HTTPException as e:
                    span.tag(HTTP_STATUS_CODE, e.status)
                    raise

                span.tag(HTTP_STATUS_CODE, resp.status)
                return resp

        return aiozipkin_middleware
    return middleware_factory


def setup(app, tracer,
          tracer_key=APP_AIOZIPKIN_KEY,
          request_key=REQUEST_AIOZIPKIN_KEY):
    app[tracer_key] = tracer
    app.middlewares.append(middleware_maker(tracer_key, request_key))

    # register cleanup signal to close zipkin connections
    async def close_aiozipkin(app):
        await app[tracer_key].close()
    app.on_cleanup.append(close_aiozipkin)

    return app


def get_tracer(app, tracer_key=APP_AIOZIPKIN_KEY):
    return app[tracer_key]


def request_span(request, request_key=REQUEST_AIOZIPKIN_KEY):
    return request[request_key]



def make_trace_config(tracer):

    trace_config = aiohttp.TraceConfig()

    async def on_request_start(session, trace_config_ctx, method, url,
                               headers, request_trace_config_ctx=None):
        import ipdb
        ipdb.set_trace()

        span_ctx = get_task_ctx('current_span_ctx')
        span = tracer.new_span(span_ctx)
        span.kind(SERVER)
        span.tag(HTTP_METHOD, method.upper())

        span_name = '{0} {1}'.format(method, '')
        span.name(span_name)
        span.start()
        trace_config_ctx._span = span

    async def on_request_end(session, trace_config_ctx, resp,
                             request_trace_config_ctx=None):
        import ipdb
        ipdb.set_trace()
        span = trace_config_ctx._span
        span.finish()

    async def on_request_redirect(session, trace_config_ctx, *a, **kw):
        import ipdb
        ipdb.set_trace()
        span = trace_config_ctx._span
        span.annotate(HTTP_REDIRECT)

    async def on_request_exception(session, trace_config_ctx, resp, error,
                                   request_trace_config_ctx=None):
        import ipdb
        ipdb.set_trace()
        span = trace_config_ctx._span
        span.finish(exception=error)

    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_end.append(on_request_end)
    trace_config.on_request_end.append(on_request_exception)
    return trace_config
