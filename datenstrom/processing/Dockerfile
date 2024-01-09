FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PATH=/venv/bin:$PATH
ENV ASSET_DIR=/app/assets

RUN apt-get -y update; apt-get -y install wget
RUN mkdir assets && cd assets && wget https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb

COPY datenstrom.processing/enricher_bin.pex /app/enricher_bin.pex

RUN PEX_TOOLS=1 /usr/local/bin/python3.11 /app/enricher_bin.pex venv /venv

EXPOSE 8000

CMD ["enricher_bin.pex/__main__.py"]