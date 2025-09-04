FROM apache/superset:5.0.0

USER root

COPY --chown=superset superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset