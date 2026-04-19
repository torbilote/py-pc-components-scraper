FROM public.ecr.aws/lambda/python:3.13

# Install uv and uvx from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy the function code to the image
COPY . ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using uv into the Lambda task root
RUN uv export --no-dev --no-hashes -o requirements.txt && \
    uv pip install -r requirements.txt \
    --target ${LAMBDA_TASK_ROOT} \
    --no-cache

# Disable development dependencies
ENV UV_NO_DEV=1


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app/orchestrator.handler" ]



