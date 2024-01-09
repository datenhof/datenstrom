FROM python:3.11-slim

WORKDIR /app
ENV PATH=/venv/bin:$PATH
ENV ASSET_DIR=/app/assets

COPY datenstrom.collector/collector_bin.pex /app/collector_bin.pex

RUN PEX_TOOLS=1 /usr/local/bin/python3.11 /app/collector_bin.pex venv /venv

EXPOSE 8000

CMD ["collector_bin.pex/__main__.py"]