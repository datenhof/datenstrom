import time
import uuid
import re
from datetime import datetime, timedelta, timezone

from urllib.parse import urlparse
from typing import Optional, List
from fastapi import APIRouter, Request, Response

from datenstrom.common.schema.raw import CollectorPayload, PayloadException

router = APIRouter()


COLLECTOR_NAME = "datenstrom-0.1.0"
PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04"
    b"\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
)
ANONYMOUS_USER_ID = "00000000-0000-0000-0000-000000000000"
HEADER_FILTER = ["remote-address", "raw-request-uri", "timeout-access"]
ANONYMOUS_HEADER_FILTER = ["cookie", "x-forwarded-for", "x-real-ip"]


def get_milliseconds():
    return int(time.time() * 1000)


def get_host_from_url(url: str) -> str:
    return urlparse(url).hostname


def make_response(request: Request, pixel: bool = False,
                  status_code: int = 200, anonymous: bool = False,
                  user_id: Optional[str] = None, redirect: Optional[str] = None):
    if pixel:
        r = Response(content=PIXEL_GIF, media_type="image/gif", status_code=status_code)
    elif redirect:
        r = Response(status_code=302)
        r.headers["location"] = redirect
    else:
        r = Response(status_code=status_code)
    if user_id and not anonymous:
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

@router.get("/")
def root():
    # request.url.hostname
    return Response(content="\U0001F44B Hello, I am you friendly neighborhood datenstrom collector",
                    media_type="text/plain")


@router.get("/health")
def health(request: Request):
    return {"i am": "ok", "hostname": request.url.hostname}


@router.get("/check_domain")
def check_domain(request: Request):
    config = request.app.config
    if not config.domain_check_regex:
        return Response(content="no domain_check_regex config", status_code=400)
    if config.domain_check_regex == "*":
        return Response(content="ok", status_code=200)
    domain = request.query_params.get("domain")
    if not domain:
        return Response(content="no domain query param", status_code=400)
    # check if domain matches regex
    if re.match(config.domain_check_regex, domain):
        return Response(content="ok", status_code=200)
    return Response(content="domain does not match domain_check_regex", status_code=400)


def get_anonymous(request: Request) -> bool:
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


@router.post("/com.snowplowanalytics.snowplow/tp2")
async def post_tp2(request: Request):
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous, user_id=e.networkUserId)


@router.get("/i")
async def get_i(request: Request):
    anonymous = get_anonymous(request)
    e = get_collector_payload(request, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, pixel=True, anonymous=anonymous, user_id=e.networkUserId)


@router.get("/r")
@router.get("/r/tp2")
async def get_r(request: Request):
    config = request.app.config
    if not config.enable_redirect_tracking:
        return make_response(request, status_code=404)
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
    return make_response(request, redirect=redirect, anonymous=anonymous, user_id=e.networkUserId)


@router.get("/{vendor}/v1")
async def get_v1(request: Request, vendor: str):
    anonymous = get_anonymous(request)
    e = get_collector_payload(request, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, pixel=True, anonymous=anonymous, user_id=e.networkUserId)


@router.get("/{vendor}/tp2")
async def vendor_post_tp2(vendor: str, request: Request):
    anonymous = get_anonymous(request)
    body = await request.body()
    e = get_collector_payload(request, body=body, anonymous=anonymous)
    write_to_sink(request, e)
    return make_response(request, anonymous=anonymous, user_id=e.networkUserId)



def add_vendor_path(path: str):
    post_path = f"/{path}/tp2"
    router.add_api_route(post_path, post_tp2, methods=["POST"])
    get_path = f"/{path}/i"
    router.add_api_route(get_path, get_i, methods=["GET"])
    redirect_path = f"/{path}/r"
    router.add_api_route(redirect_path, get_r, methods=["GET"])
