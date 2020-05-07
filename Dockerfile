FROM python:3.7

COPY . /app
WORKDIR /app
RUN chmod +x run_server.sh

RUN pip install -r requirements.txt

EXPOSE 8050

ENTRYPOINT ["/app/run_server.sh"]