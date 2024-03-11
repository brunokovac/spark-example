FROM spark:3.5.0-scala2.12-java11-ubuntu

WORKDIR /movies-app

USER root

COPY . .
RUN chmod 777 /movies-app & chown -R spark:spark /movies-app

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip; \
    pip3 install pyspark mypy fastapi uvicorn pytest pandas pyarrow;

USER spark
