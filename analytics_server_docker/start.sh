#!/bin/sh

export IP_ADDRESS=$(ip addr show wlp4s0 | awk '/inet / {print $2}' | cut -d/ -f1)

sudo -E docker-compose up -d


