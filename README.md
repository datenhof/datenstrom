# datenstrom

Event and Telemetry Tracking and Processing

## Collector

Snowplow compatible event collector

Provides a [Snowplow](https://github.com/snowplow/stream-collector) compatible event collector.
It is written in [Python](https://www.python.org/) and tries to be as simple as possible.

The collector is basically just a couple of endpoints that accept GET and POST requests, serialize the data and send it to a Kafka topic.

Endpoints:

* `/i` - (GET) Pixel Tracking requests
* `/com.vendor/track` or `/com.snowplowanalytics.snowplow/tp2` - (POST) Tracking requests (Tracker Protocol v2)
* `/com.vendor/iglu` or `/com.snowplowanalytics.iglu/v1` - (GET & POST) Self describing events (Iglu Schema)
* `/com.vendor/redirect` or `/r/tp2` - (GET) Redirect tracking
* `/health` - (GET) Health Endpoint

### Event Format

By default datenstrom is using Avro to serialize the events. The Avro schema is based on the schema that Snowplow is using. Avro is much more common in streaming (especially Kafka) environments than Thrift. If you want you can change the serialization format to Thrift (see below).

Snowplow is using [Thrift](https://thrift.apache.org/) to serialize the events. Using Thrift is needed if you want to use the Snowplow Enrichment or other parts of the Snowplow ecosystem.

Snowplow Thrift File: [Github](https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0):


### Running Locally

```
# API:
uvicorn datenstrom.collector.main:app --reload
# Enrichment
python datenstrom/enrich/app.py
```

### Building Docker Image

```
pants package datenstrom/collector:datenstrom-collector
pants publish datenstrom/collector:datenstrom-collector
```

## Enrich


### Building Docker Image

```
pants package datenstrom/processing:datenstrom-enricher
```