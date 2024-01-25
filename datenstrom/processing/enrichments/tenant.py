import requests

from requests.exceptions import ConnectionError

from typing import Any, Optional

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent
from datenstrom.common.cache import TTLCache, cachedmethod


class TenantEnrichment(BaseEnrichment):
    def __init__(self, config: Any) -> None:
        super().__init__(config=config)
        self.hostname_lookup = {}
        self.tenant_lookup_endpoint = self.config.get("tenant_lookup_endpoint")
        self.request_cache = TTLCache(maxsize=128, ttl=60)

    def lookup_hostname(self, hostname: str) -> Optional[str]:
        lower = hostname.lower()
        if lower in self.hostname_lookup:
            return self.hostname_lookup[lower]

        if self.tenant_lookup_endpoint:
            url = self.tenant_lookup_endpoint + "?hostname=" + hostname
            tenant = self.make_request(url)
            if tenant:
                self.hostname_lookup[lower] = tenant
                return tenant

    @cachedmethod(lambda self: self.request_cache)
    def make_request(self, url: str) -> Optional[str]:
        try:
            response = requests.get(url)
        except ConnectionError as e:
            print(f"Tenant lookup request connection error: {e}")
            return None
        if response.status_code == 200:
            # get tenant from response json
            try:
                return response.json()["tenant"]
            except ValueError as e:
                print(f"Tenant lookup ({url}) request json error: {e}")
                return None
            except KeyError as e:
                print(f"Tenant lookup ({url}) error - key missing in result: {e}")
                return None
        else:
            print(f"Tenant lookup ({url}) request failed: {response.status_code}")
            return None

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        if not event.raw_event.hostname:
            return
        tenant = self.lookup_hostname(event.raw_event.hostname)
        if tenant:
            event.set_value("tenant", tenant)
