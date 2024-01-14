python_distribution(
    name="datenstrom",
    dependencies=[
        "datenstrom/common:common",
        "datenstrom/connectors:connectors",
        "datenstrom/processing:processing",
        "datenstrom/settings:settings",
        "!!dependencies:poetry#confluent-kafka",
        "!!dependencies:poetry#boto3",
        "!!dependencies:poetry#fastapi",
        "!!dependencies:poetry#geoip2",
    ],
    provides=python_artifact(
        name="datenstrom",
        version="0.1.0",
    ),
    generate_setup = True,
)

python_distribution(
    name="datenstrom-server",
    dependencies=[
        "datenstrom/collector:collector",
        "datenstrom/common:common",
        "datenstrom/connectors:connectors",
        "datenstrom/processing:processing",
        "datenstrom/settings:settings",
    ],
    provides=python_artifact(
        name="datenstrom_server",
        version="0.1.0",
    ),
    generate_setup = True,
)