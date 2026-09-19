FROM python:3.11-slim

WORKDIR /app

# Même pattern que metrics-aggregator : le package partagé est installé
# séparément du requirements.txt du service, pour bénéficier du cache
# Docker (cette couche ne rebuild pas si seul le code du service change).
COPY shared/python /shared/python
RUN pip install --no-cache-dir -e /shared/python

COPY services/processing-consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/processing-consumer/src /app/src

WORKDIR /app/src

CMD ["python", "consumer.py"]