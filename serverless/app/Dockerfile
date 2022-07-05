FROM public.ecr.aws/lambda/python:3.8

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY dash_app.py ${LAMBDA_TASK_ROOT}
COPY demo/ ${LAMBDA_TASK_ROOT}/demo/
COPY assets/ ${LAMBDA_TASK_ROOT}/assets/

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ] 