FROM python:3.9.17

WORKDIR /

COPY / .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.pipeline"]