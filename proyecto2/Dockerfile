FROM public.ecr.aws/lambda/python:3.12

COPY /ingesta/lambdas/person-connect/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

COPY /ingesta/lambdas/person-connect/src/ ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
