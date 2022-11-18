FROM python:3.8
ARG ADDITIONAL_DEPENDENCIES

RUN pip3 install $ADDITIONAL_DEPENDENCIES

COPY ./app/ ./
EXPOSE 8080
ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:8080", "--workers=1", "app:server"]
