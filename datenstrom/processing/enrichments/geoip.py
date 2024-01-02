import requests
import os
import geoip2.database

from typing import Any

from datenstrom.processing.enrichments.base import BaseEnrichment, TemporaryAtomicEvent


# get directory of this file
dir_path = os.path.dirname(os.path.realpath(__file__))
geoip_file = os.path.join(dir_path, "GeoLite2-City.mmdb")


def download_file(url: str, local_file: str):
    # local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    print(f"Downloading {url} to {local_file}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_file


class GeoIPEnrichment(BaseEnrichment):
    def __init__(self, config: Any) -> None:
        super().__init__(config=config)
        self.geoip_db_url = self.config.get("geoip_db_url")
        self.read_db()

    def read_db(self):
        if not os.path.exists(geoip_file):
            download_file(self.geoip_db_url, geoip_file)
        self.reader = geoip2.database.Reader(geoip_file)

    def lookup_ip(self, ip: str):
        return self.reader.city(ip)

    def enrich(self, event: TemporaryAtomicEvent) -> None:
        if "user_ipaddress" in event:
            try:
                data = self.lookup_ip(event["user_ipaddress"])
            except geoip2.errors.AddressNotFoundError:
                return
            event.set_value("geo_country", data.country.iso_code)
            event.set_value("geo_region", data.subdivisions.most_specific.iso_code)
            event.set_value("geo_city", data.city.name)
