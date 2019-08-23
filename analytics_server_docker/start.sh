#!/bin/sh

export IP_ADDRESS=$(ip addr show wlp4s0 | awk '/inet / {print $2}' | cut -d/ -f1)

export GOOGLE_MAP_API_KEY=AIzaSyCHKNpfCWryDzxbHMN_jKX05_PF-O1BTlg

sudo -E docker-compose up -d


