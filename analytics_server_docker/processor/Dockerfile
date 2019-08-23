FROM python:3.6

WORKDIR /home/python-processor-module

COPY processor.zip .

RUN apt-get update && apt-get install unzip

RUN unzip processor.zip && rm processor.zip

RUN pip install -r requirements.txt

CMD ["python", "usecasecode/processor/stream_process.py","--sconfig=/home/python-processor-module/config/config_processor_stream_docker.json","--config=/home/python-processor-module/config/config_processor.json"]
