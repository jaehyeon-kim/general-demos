FROM python:3.8-slim

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8000
EXPOSE 50001

CMD ["uvicorn", "app:app", "--port", "8000"]
