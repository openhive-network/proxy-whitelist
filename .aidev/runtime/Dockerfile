# The image AIDEV runs this project's suites in: common-ci-configuration's
# python-test-tools (python, shellcheck, sqlfluff, psql, pytest, postgrest, curl)
# plus psycopg for the python helpers and HAF's install_with_app_lock.py, which
# scripts/install_app.sh execs itself through when it is present.
ARG BASE_IMAGE
FROM registry.gitlab.syncad.com/hive/common-ci-configuration/psql:3839feb0c30846aa3ae262143f216a3c1a0fa20b@sha256:6a3794d9fdc4e0590789fc01e48539bcd73273715e3b69dd1f45d3ecde8b1cde AS psql
FROM ${BASE_IMAGE}
COPY --from=psql /usr/local/bin/install_with_app_lock.py /usr/local/bin/install_with_app_lock.py
RUN pip install --no-cache-dir 'psycopg[binary]'
ENV HOME=/tmp PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT []
CMD ["bash"]
