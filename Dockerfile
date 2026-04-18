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
CMD [ "app/orchestrator/handler.handler" ]



# You can then locally test your function using the docker build and docker run commands.
# To build you image:
# docker build -t <image name> .

# To run your image locally:
# docker run -p 9000:8080 -e PROXY_URL_1=aaa -e PROXY_URL_2=bbb <image name>

# In a separate terminal, you can then locally invoke the function using cURL:
# curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"payload":"hello world!"}'

# Run this to build the image properly for AWS Lambda 
# docker build -t torbilote-dev/py-pc-components-scraper --provenance=false --platform linux/amd64 .