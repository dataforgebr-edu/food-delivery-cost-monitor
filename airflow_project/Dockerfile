FROM astrocrpublic.azurecr.io/runtime:3.2-4

COPY pipeline/ /usr/local/airflow/pipeline/
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/pipeline"
