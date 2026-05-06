FROM public.ecr.aws/lambda/python:3.12

COPY proyecto2/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

COPY proyecto2/src/ ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
