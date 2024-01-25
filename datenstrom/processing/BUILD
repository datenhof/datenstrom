python_sources()

pex_binary(
  name="enricher_bin",
  entry_point="datenstrom.processing.enricher",
  env={"MY_ENV_VAR": "1"},
  dependencies=["datenstrom/processing/enricher.py"],
  interpreter_constraints=["==3.11.*"],
  include_tools=True,
  layout = "packed",
  execution_mode = "venv",
)

docker_image(
    name="datenstrom-enricher",
    image_tags=("latest",),
    dependencies=[":enricher_bin"],
    repository="mths/datenstrom-enricher",
)