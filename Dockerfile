FROM public.ecr.aws/lambda/python:3.11
# FROM python:3.11-slim

RUN pip install --upgrade pip --no-cache-dir

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir \
    --target ${LAMBDA_TASK_ROOT} \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all:

COPY orchestrator.py ${LAMBDA_TASK_ROOT}/

CMD ["orchestrator.handler"]