FROM dockerhub.apps.cp.meteoswiss.ch/mch/python/builder:latest as builder
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

COPY poetry.lock pyproject.toml /src/app-root/

WORKDIR /src/app-root

RUN poetry export -o requirements.txt --without-hashes \
    && poetry export --dev -o requirements_dev.txt --without-hashes


FROM dockerhub.apps.cp.meteoswiss.ch/mch/python-3.11:latest-slim AS base
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}
ENV VERSION=$VERSION

COPY --from=builder /src/app-root/requirements.txt /src/app-root/requirements.txt

RUN apt-get -yqq update && apt-get install -yqq wget libeccodes-dev

WORKDIR /src/app-root

RUN pip install -r requirements.txt --no-cache-dir --no-deps --root-user-action=ignore

COPY flexpart_ifs_preprocessor /src/app-root/flexpart_ifs_preprocessor

WORKDIR /src


FROM base AS tester
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

COPY --from=builder /src/app-root/requirements_dev.txt /src/app-root/requirements_dev.txt
RUN pip install -r /src/app-root/requirements_dev.txt --no-cache-dir --no-deps --root-user-action=ignore

COPY pyproject.toml /src/app-root/
COPY test /src/app-root/test


FROM base AS runner
ARG VERSION
LABEL ch.meteoswiss.project=flexpart-ifs-preprocessor-${VERSION}

RUN mkdir -p /src/app-root/db && chown -R 1001:1001 /src/app-root/

# For running outside of OpenShift, we want to make sure that the container is run without root privileges
# uid 1001 is defined in the base-container-images for this purpose
USER 1001

ENTRYPOINT ["python", "-m", "flexpart_ifs_preprocessor"]
CMD []
