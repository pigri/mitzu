FROM public.ecr.aws/lambda/python:3.8
ARG ADDITIONAL_DEPENDENCIES

RUN pip3 install $ADDITIONAL_DEPENDENCIES

COPY ./app/ ./

CMD  ["gunicorn", "-b", "0.0.0.0:80", "--workers=1", "app:server"]

