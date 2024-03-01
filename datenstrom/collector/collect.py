import time
import uuid
from datetime import datetime, timedelta, timezone

from urllib.parse import urlparse
from typing import Optional, List, Dict, Any, NamedTuple
from fastapi import Request, Response

from datenstrom.common.schema.raw import CollectorPayload, PayloadException
from datenstrom.common.cache import CachedRequestClient


httpclient = CachedRequestClient(maxsize=2048, ttl=3600, none_ttl=300)


COLLECTOR_NAME = "datenstrom-0.1.0"
PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04"
    b"\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
)
ANONYMOUS_USER_ID = "00000000-0000-0000-0000-000000000000"
HEADER_FILTER = ["remote-address", "raw-request-uri", "timeout-access"]
ANONYMOUS_HEADER_FILTER = ["cookie", "x-forwarded-for", "x-real-ip"]

ENABLE_COOKIES_DEFAULT = False


class RemoteCollectorConfig(NamedTuple):
    enable_cookies: bool


def get_milliseconds():
    return int(time.time() * 1000)


def get_host_from_url(url: str) -> str:
    return urlparse(url).hostname


def make_response(request: Request, pixel: bool = False,
                  status_code: int = 200, anonymous: bool = False,
                  user_id: Optional[str] = None, redirect: Optional[str] = None,
                  collector_config: Optional[RemoteCollectorConfig] = None) -> Response:
    if pixel:
        r = Response(content=PIXEL_GIF, media_type="image/gif", status_code=status_code)
    elif redirect:
        r = Response(status_code=302)
        r.headers["location"] = redirect
    else:
        r = Response(status_code=status_code)

    # check if we should set a cookie
    if anonymous:
        return r
    if collector_config and not collector_config.enable_cookies:
        return r
    # enable / disable cookies per default
    if not collector_config and not ENABLE_COOKIES_DEFAULT:
        return r
    if user_id:
        # get Origin from header
        origin = request.headers.get("Origin")
        # extract host from origins
        host = get_host_from_url(origin)
        # get possible domains from config
        cookie_domain = None
        cookie_domains = request.app.config.cookie_domains or []
        for c in cookie_domains:
            if host.endswith(c):
                cookie_domain = host
                break
        if not cookie_domain and request.app.config.cookie_fallback_domain:
            cookie_domain = request.app.config.cookie_fallback_domain
        r.set_cookie(
            key=request.app.config.cookie_name,
            value=user_id,
            expires=datetime.now(timezone.utc) + timedelta(days=request.app.config.cookie_expiration_days),
            domain=cookie_domain,
            secure=request.app.config.cookie_secure,
            httponly=request.app.config.cookie_http_only,
            samesite=request.app.config.cookie_same_site,
        )
    return r


def get_collector_config(request: Request) -> Optional[RemoteCollectorConfig]:
    app_config = request.app.config
    if app_config.remote_config_endpoint:
        url = f"{app_config.remote_config_endpoint}?hostname={request.url.hostname}"
        config = httpclient.get_json(url, timeout=5)
        if config:
            if "enable_cookies" in config and isinstance(config["enable_cookies"], bool):
                return RemoteCollectorConfig(enable_cookies=config["enable_cookies"])
    return None


def get_anonymous(request: Request) -> bool:
    # check headers
    if request.headers.get("sp-anonymous"):
        return True
    if request.headers.get("anonymous"):
        return True
    return False


def get_headers(request: Request, anonymous: bool = False) -> List[str]:
    headers = []
    for name, value in request.headers.items():
        if name.lower() in HEADER_FILTER:
            continue
        if anonymous and name.lower() in ANONYMOUS_HEADER_FILTER:
            continue
        headers.append(f"{name}: {value}")
    return headers


def get_network_userid(request: Request, anonymous: bool = False,
                       cookie_user_id: Optional[str] = None) -> Optional[str]:
    if anonymous:
        return ANONYMOUS_USER_ID
    # get nuid from query string
    nuid = request.query_params.get("nuid")
    if nuid:
        return nuid
    if cookie_user_id:
        return cookie_user_id
    # generate new user id
    return str(uuid.uuid4())


def get_tracking_cookie(request: Request) -> Optional[str]:
    cookie_name = request.app.config.cookie_name
    cookie = request.cookies.get(cookie_name)
    if cookie:
        return cookie


def write_to_sink(request: Request, e: CollectorPayload):
    sink = request.app.state.sink
    config = request.app.config
    try:
        size = sink.write(e.split_and_serialize(config.record_format, max_size=config.max_bytes))
    except PayloadException as e:
        print(f"PayloadException: {e}", flush=True)
    else:
        print(f"wrote {size} bytes to sink", flush=True)


def get_collector_payload(request: Request, body: Optional[bytes] = None,
                          anonymous: bool = False) -> CollectorPayload:
    ip = request.client.host
    path = request.url.path
    user_agent = request.headers.get("user-agent")
    hostname = request.url.hostname
    querystring = request.url.query
    cookie = get_tracking_cookie(request)
    referer = request.headers.get("referer")
    content_type = request.headers.get("content-type")
    e = CollectorPayload(
        ipAddress=ip,
        timestamp=get_milliseconds(),
        encoding="UTF-8",
        collector=COLLECTOR_NAME,
    )
    if querystring:
        e.querystring = querystring
    if body:
        e.body = body
    e.path = path
    if user_agent:
        e.userAgent = user_agent
    if referer:
        e.refererUri = referer
    e.hostname = hostname
    e.networkUserId = get_network_userid(request, anonymous=anonymous, cookie_user_id=cookie)
    e.headers = get_headers(request, anonymous=anonymous)
    if content_type:
        e.contentType = content_type
    return e
