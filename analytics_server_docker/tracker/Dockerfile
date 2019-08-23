FROM python:3.6

WORKDIR /home/python-tracker-module

COPY tracker.zip .

RUN apt-get update && apt-get install unzip

RUN unzip tracker.zip && rm tracker.zip

RUN pip install -r requirements.txt

CMD ["python", "usecasecode/tracker/stream_track.py","--sconfig=/home/python-tracker-module/config/config_tracker_stream_docker.json","--config=/home/python-tracker-module/config/config_tracker.json"]
