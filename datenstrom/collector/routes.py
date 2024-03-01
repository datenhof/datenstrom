import time
import uuid
from datetime import datetime, timedelta, timezone

from urllib.parse import urlparse
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Request, Response

from datenstrom.common.schema.raw import CollectorPayload, PayloadException
from datenstrom.common.cache import CachedRequestClient
from datenstrom.collector.collect import (
    make_response, get_anonymous,
    get_collector_payload, write_to_sink,
    get_collector_config
)

router = APIRouter()


@router.get("/")
def root():
    # request.url.hostname
    return Response(content="\U0001F44B Hello, I am you friendly neighborhood datenstrom collector",
                    media_type="text/plain")


@router.get("/health")
def health(request: Request):
    return {"i am": "ok", "hostname": request.url.hostname}


@router.post(
    "/com.snowplowanalytics.snowplow/tp2",
    name="Snowplow POST endpoint"
)
async def post_tp2(request: Request):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous,
                         user_id=e.networkUserId, collector_config=collector_config)


@router.get(
    "/i",
    responses={
        200: {
            "content": {"image/gif": {}},
            "description": "1x1 pixel"
        },
    },
    name="Pixel Tracker (snowplow compatible)"
)
async def get_i(request: Request):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    e = get_collector_payload(request, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, pixel=True,
                         anonymous=anonymous, user_id=e.networkUserId,
                         collector_config=collector_config)


@router.post(
    "/event",
    name="Event tracking endpoint"
)
async def post_event(request: Request):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous,
                         user_id=e.networkUserId,
                         collector_config=collector_config)


async def get_r(request: Request):
    collector_config = get_collector_config(request)
    # get u query parameter
    u = request.query_params.get("u")
    # check if u is set
    if not u:
        return make_response(request, status_code=400)
    # parse u and check if it is a valid uri
    try:
        u = urlparse(u)
    except ValueError:
        return make_response(request, status_code=400)
    if not u.scheme:
        return make_response(request, status_code=400)
    redirect = u.geturl()
    anonymous = get_anonymous(request)
    e = get_collector_payload(request, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, redirect=redirect,
                         anonymous=anonymous, user_id=e.networkUserId,
                         collector_config=collector_config)


@router.get(
    "/{vendor}/v1",
    name="Iglu GET Endpoint"
)
async def get_v1(request: Request, vendor: str):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    e = get_collector_payload(request, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, pixel=True, anonymous=anonymous,
                         user_id=e.networkUserId,
                         collector_config=collector_config)


@router.post(
    "/{vendor}/v1",
    name="Iglu POST Endpoint"
)
async def post_v1(request: Request, vendor: str):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous,
                         user_id=e.networkUserId,
                         collector_config=collector_config)


@router.post(
    "/{vendor}/tp2",
    name="Snowplow POST endpoint (custom vendor)"
    )
async def vendor_post_tp2(vendor: str, request: Request):
    collector_config = get_collector_config(request)
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous,
                         user_id=e.networkUserId, collector_config=collector_config)


def add_redirect_routes():
    router.add_api_route("/r", get_r, methods=["GET"])
    router.add_api_route("/r/tp2", get_r, methods=["GET"])


def add_vendor_path(path: str):
    post_path = f"/{path}/tp2"
    router.add_api_route(post_path, post_tp2, methods=["POST"])
    get_path = f"/{path}/i"
    router.add_api_route(get_path, get_i, methods=["GET"])
    redirect_path = f"/{path}/r"
    router.add_api_route(redirect_path, get_r, methods=["GET"])
