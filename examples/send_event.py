from snowplow_tracker import (
    Snowplow, EmitterConfiguration, Subject, TrackerConfiguration,
    SelfDescribingJson, SelfDescribing, PageView, StructuredEvent
)

tracker_config = TrackerConfiguration()
emitter_config = EmitterConfiguration()
s = Subject()
s.set_user_id('user_id')
t = Snowplow.create_tracker(namespace='namespace', endpoint='http://localhost:8000',
                            tracker_config=tracker_config, emitter_config=emitter_config,
                            subject=s)
page_view = PageView(
  page_url="http://www.example.com",
  page_title="title",
)
t.track(page_view)

link_click = SelfDescribing(
  SelfDescribingJson(
    "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
    {"targetUrl": "https://www.snowplow.io"},
  ),
)
t.track(link_click)

se = StructuredEvent(category="cat", action="act",
                     value="val", label="lab")
t.track(se)

t.flush()