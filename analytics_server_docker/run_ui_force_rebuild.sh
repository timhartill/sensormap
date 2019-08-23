#!/bin/sh
#NOTE: This script is no longer needed.  Previously, if ip address changed, you had to rebuild the ui docker container. docker-compose will automatically rebuild if an image is missing so delete the ui image before running ./start.sh to accomplish this.. 

docker rmi --force analytics_server_docker_ui:latest


