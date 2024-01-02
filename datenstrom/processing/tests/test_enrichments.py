from datenstrom.processing.enrichments.geoip import GeoIPEnrichment

def test_geoip():
    config = {
        "geoip_db_url": "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb"
    }
    g = GeoIPEnrichment(config=config)
    data = g.lookup_ip("3.65.192.185")
    assert data.country.iso_code == "DE"
    assert data.subdivisions.most_specific.iso_code == "HE"
    assert data.city.name == "Frankfurt am Main"
