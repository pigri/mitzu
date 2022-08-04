FROM public.ecr.aws/lambda/python:3.8
ARG ADDITIONAL_DEPENDENCIES

COPY requirements.txt  ./requirements.txt
RUN pip3 install -r requirements.txt
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
RUN pip3 install $ADDITIONAL_DEPENDENCIES
RUN pip3 install $ADDITIONAL_DEPENDENCIES --target "${LAMBDA_TASK_ROOT}"

COPY . ./
COPY . ${LAMBDA_TASK_ROOT}
