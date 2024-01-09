python_sources()

python_requirement(name="uvicorn", requirements=["uvicorn[standard]>=0.25.0,<1.0", "gunicorn>=21.2.0,<22.0.0"])

pex_binary(
  name="collector_bin",
  script="gunicorn",
  args=["-k uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "datenstrom.collector.main:app"],
  env={"MY_ENV_VAR": "1"},
  dependencies=[":uvicorn", "datenstrom/collector/main.py"],
  interpreter_constraints=["==3.11.*"],
  include_tools=True,
  layout = "packed",
  execution_mode = "venv",
)

docker_image(
    name="datenstrom-collector",
    image_tags=("latest",),
    dependencies=[":collector_bin"],
    repository="mths/datenstrom-collector",
)