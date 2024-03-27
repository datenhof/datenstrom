PAYLOAD_DATA_SCHEMA = {
    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    "description": "Schema for a Snowplow payload",
    "self": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "payload_data",
        "format": "jsonschema",
        "version": "1-0-4"
    },

    "type": "array",
    "items":{

        "type": "object",
        "properties": {
            "tna": {
                "type": "string"
            },
            "aid": {
                "type": "string"
            },
            "p": {
                "type": "string"
            },
            "dtm": {
                "type": "string"
            },
            "tz": {
                "type": "string"
            },
            "e": {
                "type": "string"
            },
            "tid": {
                "type": "string"
            },
            "eid": {
                "type": "string"
            },
            "tv": {
                "type": "string"
            },
            "duid": {
                "type": "string"
            },
            "nuid": {
                "type": "string"
            },
            "uid": {
                "type": "string"
            },
            "vid": {
                "type": "string"
            },
            "ip": {
                "type": "string"
            },
            "res": {
                "type": "string"
            },
            "url": {
                "type": "string"
            },
            "page": {
                "type": "string"
            },
            "refr": {
                "type": "string"
            },
            "fp": {
                "type": "string"
            },
            "ctype": {
                "type": "string"
            },
            "cookie": {
                "type": "string"
            },
            "lang": {
                "type": "string"
            },
            "f_pdf": {
                "type": "string"
            },
            "f_qt": {
                "type": "string"
            },
            "f_realp": {
                "type": "string"
            },
            "f_wma": {
                "type": "string"
            },
            "f_dir": {
                "type": "string"
            },
            "f_fla": {
                "type": "string"
            },
            "f_java": {
                "type": "string"
            },
            "f_gears": {
                "type": "string"
            },
            "f_ag": {
                "type": "string"
            },
            "cd": {
                "type": "string"
            },
            "ds": {
                "type": "string"
            },
            "cs": {
                "type": "string"
            },
            "vp": {
                "type": "string"
            },
            "mac": {
                "type": "string"
            },
            "pp_mix": {
                "type": "string"
            },
            "pp_max": {
                "type": "string"
            },
            "pp_miy": {
                "type": "string"
            },
            "pp_may": {
                "type": "string"
            },
            "ad_ba": {
                "type": "string"
            },
            "ad_ca": {
                "type": "string"
            },
            "ad_ad": {
                "type": "string"
            },
            "ad_uid": {
                "type": "string"
            },
            "tr_id": {
                "type": "string"
            },
            "tr_af": {
                "type": "string"
            },
            "tr_tt": {
                "type": "string"
            },
            "tr_tx": {
                "type": "string"
            },
            "tr_sh": {
                "type": "string"
            },
            "tr_ci": {
                "type": "string"
            },
            "tr_st": {
                "type": "string"
            },
            "tr_co": {
                "type": "string"
            },
            "tr_cu": {
                "type": "string"
            },
            "ti_id": {
                "type": "string"
            },
            "ti_sk": {
                "type": "string"
            },
            "ti_nm": {
                "type": "string"
            },
            "ti_na": {
                "type": "string"
            },
            "ti_ca": {
                "type": "string"
            },
            "ti_pr": {
                "type": "string"
            },
            "ti_qu": {
                "type": "string"
            },
            "ti_cu": {
                "type": "string"
            },
            "sa": {
                "type": "string"
            },
            "sn": {
                "type": "string"
            },
            "st": {
                "type": "string"
            },
            "sp": {
                "type": "string"
            },
            "se_ca": {
                "type": "string"
            },
            "se_ac": {
                "type": "string"
            },
            "se_la": {
                "type": "string"
            },
            "se_pr": {
                "type": "string"
            },
            "se_va": {
                "type": "string"
            },
            "ue_na": {
                "type": "string"
            },
            "ue_pr": {
                "type": "string"
            },
            "ue_px": {
                "type": "string"
            },
            "co": {
                "type": "string"
            },
            "cx": {
                "type": "string"
            },
            "ua": {
                "type": "string"
            },
            "tnuid": {
                "type": "string"
            },
            "stm": {
                "type": "string"
            },
            "sid": {
                "type": "string"
            },
            "ttm": {
                "type": "string"
            }
        },
        "required": ["tv", "p", "e"],
        "additionalProperties": False
    },
    "minItems": 1
}

CONTEXTS_SCHEMA = {
    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    "description": "Schema for custom contexts",
    "self": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "contexts",
        "format": "jsonschema",
        "version": "1-0-1"
    },

    "type": "array",

    "items": {

        "type": "object",

        "properties": {

            "schema": {
                "type": "string",
                "pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$"
            },

            "data": {}
        },

        "required": ["schema", "data"],
        "additionalProperties": False
    }
}

UNSTRUCT_EVENT_SCHEMA = {
    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
    "description": "Schema for a Snowplow unstructured event",
    "self": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "unstruct_event",
        "format": "jsonschema",
        "version": "1-0-0"
    },

    "type": "object",

    "properties": {

        "schema": {
            "type": "string",
            "pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$"
        },

        "data": {}
    },

    "required": ["schema", "data"],
    "additionalProperties": False
}

PAGE_VIEW_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a page view",
    "self": {
        "vendor": "io.datenstrom",
        "name": "page_view",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "page_url": {
            "type": ["string", "null"],
        },
        "page_title": {
            "type": ["string", "null"],
        },
        "page_referrer": {
            "type": ["string", "null"],
        },
    },
    "required": ["page_url"],
    "additionalProperties": False
}

PAGE_PING_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a page ping",
    "self": {
        "vendor": "io.datenstrom",
        "name": "page_ping",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "pp_xoffset_min": {
            "type": ["integer", "null"]
        },
        "pp_xoffset_max": {
            "type": ["integer", "null"]
        },
        "pp_yoffset_min": {
            "type": ["integer", "null"]
        },
        "pp_yoffset_max": {
            "type": ["integer", "null"]
        },
    },
    "additionalProperties": False
}


STRUCTURED_EVENT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a structured default event",
    "self": {
        "vendor": "io.datenstrom",
        "name": "structured_event",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "category": {
            "type": "string",
            "maxLength": 150
        },
        "action": {
            "type": "string",
            "maxLength": 500
        },
        "label": {
            "type": [
                "string",
                "null"
            ],
            "maxLength": 500
        },
        "value": {
            "type": [
                "string",
                "null"
            ],
            "maxLength": 500
        },
        "property": {
            "type": [
                "string",
                "null"
            ],
            "maxLength": 500
        }
    },
    "required": ["category", "action"],
    "additionalProperties": False
}


TRANSACTION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a transaction",
    "self": {
        "vendor": "io.datenstrom",
        "name": "transaction",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "txn_id": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_id": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_af": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_tt": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_tx": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_sh": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_ci": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_st": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_co": {
            "type": [
                "string",
                "null"
            ]
        },
        "tr_cu": {
            "type": [
                "string",
                "null"
            ]
        },
    },
    "additionalProperties": False
}


TRANSACTION_ITEM_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a transaction item",
    "self": {
        "vendor": "io.datenstrom",
        "name": "transaction_item",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "ti_id": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_sk": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_nm": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_na": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_ca": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_pr": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_qu": {
            "type": [
                "string",
                "null"
            ]
        },
        "ti_cu": {
            "type": [
                "string",
                "null"
            ]
        },
    },
    "additionalProperties": False
}


CAMPAIGN_ATTRIBUTION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for a campaign context",
    "self": {
        "vendor": "io.datenstrom",
        "name": "campaign_attribution",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "medium" : {
            "type": [
                "string",
                "null"
            ]
        },
        "source" : {
            "type": [
                "string",
                "null"
            ]
        },
        "campaign" : {
            "type": [
                "string",
                "null"
            ]
        },
        "term" : {
            "type": [
                "string",
                "null"
            ]
        },
        "content" : {
            "type": [
                "string",
                "null"
            ]
        },
        "click_id" : {
            "type": [
                "string",
                "null"
            ]
        },
        "network" : {
            "type": [
                "string",
                "null"
            ]
        },
    },
    "additionalProperties": False
}


DEVICE_INFO_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Schema for device information",
    "self": {
        "vendor": "io.datenstrom",
        "name": "device_info",
        "format": "jsonschema",
        "version": "1-0-0"
    },
    "type": "object",
    "properties": {
        "screen_resolution": {
            "type": [
                "string",
                "null"
            ]
        },
        "viewport_resolution": {
            "type": [
                "string",
                "null"
            ]
        },
        "browser_family": {
            "type": [
                "string",
                "null"
            ]
        },
        "browser_version": {
            "type": [
                "string",
                "null"
            ]
        },
        "os_family": {
            "type": [
                "string",
                "null"
            ]
        },
        "os_version": {
            "type": [
                "string",
                "null"
            ]
        },
        "device_family": {
            "type": [
                "string",
                "null"
            ]
        },
    },
    "additionalProperties": False
}


STATIC_JSON_SCHEMAS = {
    "com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-0": PAYLOAD_DATA_SCHEMA,
    "com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-1": PAYLOAD_DATA_SCHEMA,
    "com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-2": PAYLOAD_DATA_SCHEMA,
    "com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-3": PAYLOAD_DATA_SCHEMA,
    "com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4": PAYLOAD_DATA_SCHEMA,

    "com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0": UNSTRUCT_EVENT_SCHEMA,
    "com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0": CONTEXTS_SCHEMA,
    "com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1": CONTEXTS_SCHEMA,

    "io.datenstrom/page_view/jsonschema/1-0-0": PAGE_VIEW_SCHEMA,
    "io.datenstrom/page_ping/jsonschema/1-0-0": PAGE_PING_SCHEMA,
    "io.datenstrom/structured_event/jsonschema/1-0-0": STRUCTURED_EVENT_SCHEMA,
    "io.datenstrom/transaction/jsonschema/1-0-0": TRANSACTION_SCHEMA,
    "io.datenstrom/transaction_item/jsonschema/1-0-0": TRANSACTION_ITEM_SCHEMA,

    "io.datenstrom/campaign_attribution/jsonschema/1-0-0": CAMPAIGN_ATTRIBUTION_SCHEMA,
    "io.datenstrom/device_info/jsonschema/1-0-0": DEVICE_INFO_SCHEMA,
}