import jwt

from unittest.mock import patch

from datenstrom.processing.enrichments.geoip import GeoIPEnrichment
from datenstrom.processing.enrichments.authentication import AuthenticationEnrichment
from datenstrom.processing.enrichments.campaign import CampaignEnrichment
from datenstrom.processing.enrichments.base import TemporaryAtomicEvent, CollectorPayload
from datenstrom.settings import get_test_settings


KEY_PRIVATE = """-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQCUubE0NdZh4wv5uuLiwCLlag80QID6+6Qa2cbAapP94F75IDJ+
G4Y0rkLUqMBchWQ/UkiqDcJdk/f+TW/2Wd7zCzW2n9vFXJ3PR9AKOzcONQdymQ6J
T4c3TeEL/6RL9qhYyxlUgzXJYrRII+GVWftzSrhWK2XZsA6buyhWP9wwMwIDAQAB
AoGAe0iqofL28VG6fZrztK88vht62v+Va3fpgvB/lsVCRDMXxz9vW6YJS+YgNBRm
0MsqXGsjHQQm2FduPXmHlBjPfDR+oPM15tor6r7To5O7iKG1tOI57qIAu7K/G3hW
sF4J3EBi8YcY7+IY31+neGBwNVBHW+3iicnJG25uPlSnNBECQQDo4JwLyTC3WDAb
hYfIH96bA7mDU5+hVqKdhxvvrjpCZRJkOaQ0OBEM73Lg10G3w67bSg3M5x4FAqeS
Z6OZXqx3AkEAo34Q0Lc8vf0wg/TsSmY/P/+tUsTyKbFIy45prgeTozGcqfY2wASY
ayPpJ5n1DIio4ooczYYdM5+hmhz4apSVJQJAYtntA0e8qScDDLuqvnVgvbZgCfH2
QbsPvgR7BEuVqVdT7j1ViWD/Q9lkjzJwT/v9flI5929nm8LbtNbPaCv60wJAeugm
1AEslNdmRY02pafAHOik7/hG5Lj2NLDXGZFwl9qQH+WMu0W5H7JbcP34jneeHtAd
XxRXl5uF7UmiJt2UUQJBANnv33NLetHRon1FLeHp3Oueqb/5IqQA8UOUEEd89vIr
STgPtGmDTc1zSjXvvyd89pvtTo0sxELMD2MHi2JwjOQ=
-----END RSA PRIVATE KEY-----"""

KEY_PUBLIC = """-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCUubE0NdZh4wv5uuLiwCLlag80
QID6+6Qa2cbAapP94F75IDJ+G4Y0rkLUqMBchWQ/UkiqDcJdk/f+TW/2Wd7zCzW2
n9vFXJ3PR9AKOzcONQdymQ6JT4c3TeEL/6RL9qhYyxlUgzXJYrRII+GVWftzSrhW
K2XZsA6buyhWP9wwMwIDAQAB
-----END PUBLIC KEY-----"""


def generate_jwt_token():
    token = jwt.encode(
        {"iss": "https://datenstrom.io",
         "sub": "my_user"},
        KEY_PRIVATE,
        algorithm="RS256",
    )
    return token

CONFIG = get_test_settings()
CONFIG.authentication_public_key = KEY_PUBLIC
CONFIG.download_geoip_db = True


TEST_EVENT1 = CollectorPayload(
    ipAddress="3.65.192.185",
    timestamp=1614355200000,
    encoding="utf-8",
    collector="test",
    userAgent="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    refererUri="https://www.google.com/",
    path="/",
    querystring="",
    body="",
    headers=["authorization: Bearer " + generate_jwt_token()],
    contentType="",
    hostname="example.com",
    networkUserId=""
)


def test_geoip():
    g = GeoIPEnrichment(config=CONFIG)
    data = g.lookup_ip("3.65.192.185")
    assert data.country.iso_code == "DE"
    assert data.subdivisions.most_specific.iso_code == "HE"
    assert data.city.name == "Frankfurt am Main"


def test_authentication():
    a = AuthenticationEnrichment(config=CONFIG)
    ev = TemporaryAtomicEvent(TEST_EVENT1)
    a.enrich(ev)
    assert ev["collector_auth"] == "my_user"
