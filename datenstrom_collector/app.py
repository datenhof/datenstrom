from fastapi import FastAPI, Request, Response

from datenstrom_collector.settings import config


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

    app.config = config

    app.middleware("http")(cors_preflight)

    from datenstrom_collector.sinks.dev import DevSink
    from datenstrom_collector.sinks.kafka import KafkaSink

    if config.sink == "kafka":
        app.state.sink = KafkaSink(config=config)
    elif config.sink == "dev":
        app.state.sink = DevSink(config=config)
    else:
        raise ValueError(f"Unknown sink: {config.sink}")

    from datenstrom_collector.routers import core

    for vendor in config.vendors:
        core.add_vendor_path(vendor)
    app.include_router(core.router)

    return app
