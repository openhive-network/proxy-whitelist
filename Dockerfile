# syntax=docker/dockerfile:1.5
# Pinned to the c-c-c develop SHA tag that introduces python3 + py3-psycopg2
# + /usr/local/bin/install_with_app_lock.py (the wrapper used by install_app.sh).
# Bump when c-c-c publishes a new semver tag that includes the wrapper.
ARG PSQL_CLIENT_VERSION=5057f1b6f8f1c37d5b6e39015746bd526805cb76
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/psql:${PSQL_CLIENT_VERSION} AS psql_client

USER root
RUN apk add --no-cache curl

USER haf_admin
WORKDIR /home/haf_admin

ENTRYPOINT [ "/bin/bash", "-c" ]

FROM psql_client AS full

ARG BUILD_TIME
ARG GIT_COMMIT_SHA
ARG GIT_CURRENT_BRANCH
ARG GIT_LAST_LOG_MESSAGE
ARG GIT_LAST_COMMITTER
ARG GIT_LAST_COMMIT_DATE
LABEL org.opencontainers.image.created="$BUILD_TIME"
LABEL org.opencontainers.image.url="https://hive.io/"
LABEL org.opencontainers.image.documentation="https://gitlab.syncad.com/hive/proxy-whitelist"
LABEL org.opencontainers.image.source="https://gitlab.syncad.com/hive/proxy-whitelist"
LABEL org.opencontainers.image.revision="$GIT_COMMIT_SHA"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.ref.name="ProxyWhitelist"
LABEL org.opencontainers.image.title="Proxy Whitelist Image"
LABEL org.opencontainers.image.description="Runs proxy_whitelist HAF application for imagehoster proxy URL whitelisting"
LABEL io.hive.image.branch="$GIT_CURRENT_BRANCH"
LABEL io.hive.image.commit.log_message="$GIT_LAST_LOG_MESSAGE"
LABEL io.hive.image.commit.author="$GIT_LAST_COMMITTER"
LABEL io.hive.image.commit.date="$GIT_LAST_COMMIT_DATE"

USER root

RUN <<EOF
  set -e
  mkdir /app
  chown haf_admin /app
EOF

USER haf_admin

COPY scripts/install_app.sh /app/scripts/install_app.sh
COPY scripts/process_blocks.sh /app/scripts/process_blocks.sh
COPY db /app/db
COPY endpoints /app/endpoints
COPY docker/scripts/docker-entrypoint.sh /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
