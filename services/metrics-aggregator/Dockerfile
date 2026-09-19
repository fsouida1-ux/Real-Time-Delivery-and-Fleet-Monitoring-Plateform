FROM python:3.11-slim

WORKDIR /app

# On copie d'abord le package partagé et on l'installe séparément :
# ça permet à Docker de mettre cette couche en cache — si tu ne
# modifies que le code de metrics-aggregator, cette étape ne sera
# pas rejouée au prochain build (même logique que processing-consumer).
COPY shared/python /shared/python
RUN pip install --no-cache-dir -e /shared/python

COPY services/metrics-aggregator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/metrics-aggregator/src /app/src

WORKDIR /app/src

CMD ["python", "main.py"]