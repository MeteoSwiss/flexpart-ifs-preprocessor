# Stage 1: Builder
FROM dockerhub.apps.cp.meteoswiss.ch/mch/python/builder:latest as builder
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

COPY poetry.lock pyproject.toml /src/

RUN cd /src \
    && poetry export -o requirements.txt --without-hashes \
    && poetry export --dev -o requirements_dev.txt --without-hashes

# Stage 2: Base Image
FROM dockerhub.apps.cp.meteoswiss.ch/mch/python-3.11:latest-slim AS base
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}
ENV VERSION=$VERSION

COPY --from=builder /src/requirements.txt /src/requirements.txt

RUN apt-get -yqq update && apt-get install -yqq wget libeccodes-dev

RUN cd /src \
    && pip install -r requirements.txt

COPY flexpart_ifs_preprocessor /src/flexpart_ifs_preprocessor

WORKDIR /src

# Stage 3: Tester
FROM base AS tester
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

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
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

RUN mkdir -p /src/db && chown -R 1001:1001 /src

# For running outside of OpenShift, we want to make sure that the container is run without root privileges
# uid 1001 is defined in the base-container-images for this purpose
USER 1001

ENTRYPOINT ["python", "-m", "flexpart_ifs_preprocessor"]
CMD []
