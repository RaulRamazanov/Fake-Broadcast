FROM python:3.11-slim

WORKDIR /app

# 1. Движок — отдельный слой. Скачивается один раз, дальше Docker берёт его из кэша,
#    пока не изменится сам get_stockfish.sh. Правки кода его не трогают.
COPY get_stockfish.sh .
RUN sh ./get_stockfish.sh \
 || (apt-get update && apt-get install -y --no-install-recommends stockfish \
     && rm -rf /var/lib/apt/lists/*)

# 2. Зависимости Python — тоже из кэша, пока не изменится requirements.txt.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Код — копируется последним, поэтому пересборка после правок занимает секунды.
COPY . .

EXPOSE 5000

CMD ["python", "app.py"]