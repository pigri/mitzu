FROM python:3.10.6-slim
ARG MITZU_VERSION

WORKDIR /app


COPY ./app/ ./
COPY ./requirements.txt ./
COPY ./dist/ ./dist

RUN apt-get update

# add arm64 architecture
RUN dpkg --add-architecture arm64
RUN apt-get install -y libpq-dev gcc

RUN pip3 install mitzu==${MITZU_VERSION} --no-deps --no-index --find-links ./dist/
RUN pip3 install -r requirements.txt

EXPOSE 8080

CMD [ "gunicorn", "-b", ":8080", "-w", "1", "app:server" ]
