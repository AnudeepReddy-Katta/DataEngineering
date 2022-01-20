FROM python:3.9.1
# ENTRYPOINT [ "python", "pipeline.py" ]
# RUN pip install pandas
ENTRYPOINT [ "python", "data_ingestion.py" ]
RUN pip install pandas sqlalchemy psycopg2
WORKDIR /app
# COPY pipeline.py pipeline.py
COPY data_ingestion.py data_ingestion.py