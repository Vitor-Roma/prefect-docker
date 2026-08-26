FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY prefect.yaml .
COPY deploy_flows.py .
COPY app ./app

CMD ["python", "deploy_flows.py"]