FROM python:3.11.9-slim-bullseye
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir --prefer-binary -r requirements.txt
COPY src ./src
COPY main.py .
RUN mkdir log
ENTRYPOINT ["python3", "main.py"]