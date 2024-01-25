from fastapi import FastAPI, Request, Response

from datenstrom.settings import get_settings


async def cors_preflight(request: Request, call_next):
    if request.method == "OPTIONS":
        response = Response(status_code=200)
    else:
        response = await call_next(request)
    # try to get domain out of Origin header
    origin = request.headers.get("Origin")
    if origin:
        response.headers["Access-Control-Allow-Origin"] = origin
    else:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"] = "3600"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, SP-Anonymous, Anonymous, Origin, Referer, User-Agent"
    return response


def create_app() -> FastAPI:
    app = FastAPI()

    config = get_settings()
    app.config = config

    app.middleware("http")(cors_preflight)

    if config.transport == "kafka":
        from datenstrom.connectors.sinks.kafka import KafkaSink
        app.state.sink = KafkaSink(config=config, queue_type="raw")
    elif config.transport == "dev":
        from datenstrom.connectors.sinks.dev import DevSink
        app.state.sink = DevSink(config=config, queue_type="raw")
    elif config.transport == "sqs":
        from datenstrom.connectors.sinks.sqs import SQSSink
        app.state.sink = SQSSink(config=config, queue_type="raw")
    else:
        raise ValueError(f"Unknown transport sink: {config.transport}")

    from datenstrom.collector.routes import add_vendor_path, router

    for vendor in config.vendors:
        add_vendor_path(vendor)
    app.include_router(router)

    return app
