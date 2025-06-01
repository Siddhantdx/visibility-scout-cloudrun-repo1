FROM python:3.9-slim-buster
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
RUN pip install --upgrade pip && pip install -r requirements.txt
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
