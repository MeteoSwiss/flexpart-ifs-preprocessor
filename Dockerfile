# Stage 1: Builder
FROM dockerhub.apps.cp.meteoswiss.ch/mch/python/builder:latest as builder

COPY poetry.lock pyproject.toml /src/

RUN cd /src \
    && poetry export -o requirements.txt --without-hashes \
    && poetry export --dev -o requirements_dev.txt --without-hashes

# Stage 2: Base Image
FROM dockerhub.apps.cp.meteoswiss.ch/mch/python-3.11:latest-slim AS base

COPY --from=builder /src/requirements.txt /src/requirements.txt

RUN apt-get -yqq update && apt-get install -yqq wget libeccodes-dev

RUN cd /src \
    && pip install -r requirements.txt

COPY flexprep /src/flexprep

WORKDIR /src

# Stage 3: Tester
FROM base AS tester

COPY --from=builder /src/requirements_dev.txt /src/requirements_dev.txt
RUN pip install -r /src/requirements_dev.txt

COPY pyproject.toml test_ci.sh /src/
COPY test /src/test

CMD ["/bin/bash", "-c", "source /src/test_ci.sh && run_ci_tools"]

# Stage 4: Documenter
FROM tester AS documenter

COPY doc /src/doc
COPY HISTORY.rst README.rst /src/

CMD ["sphinx-build", "doc", "doc/_build"]

# Stage 5: Runner
FROM base AS runner

ARG VERSION
ENV VERSION=$VERSION

# Unset proxy for use outside MCH
ENV HTTP_PROXY= \
   http_proxy= \
   HTTPS_PROXY= \
   https_proxy= \
   NO_PROXY= \
   no_proxy=

# Create a non-root user and set up permissions
RUN useradd --create-home flexprep-user

RUN mkdir -p /src/db && chown -R flexprep-user:flexprep-user /src

# Switch to the non-root user
USER flexprep-user

ENTRYPOINT ["python", "-m", "flexprep"]
