FROM python:3.7

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8050

ENTRYPOINT ["run_server.sh"]