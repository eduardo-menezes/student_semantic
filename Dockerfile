FROM apache/airflow:2.7.1
COPY requirements.txt .

ENV PYTHON_VERSION=3.8
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT_URL} -r requirements.txt