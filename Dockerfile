FROM apache/airflow:3.1.7
ADD requirements.txt .
RUN pip install -r requirements.txt
