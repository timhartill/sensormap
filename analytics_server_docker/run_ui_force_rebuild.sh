#!/bin/sh
Run this script to remove the ui docker image forcing a rebuild. typically you only need to run this if you need to update the google maps api key that is to be used. 

docker rmi --force analytics_server_docker_ui:latest


