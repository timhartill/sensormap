#!/bin/bash
# Run as ENTRYPOINT["/update_config.sh", "/usr/share/nginx/html/"] when running in docker file. 
# Run as bash update_config.sh ./public/ when running in dev env
set -e
echo "window.appConfig = { GOOGLE_MAP_API_KEY: '${REACT_APP_GOOGLE_MAP_API_KEY}', BACKEND_IP_ADDRESS:'${REACT_APP_BACKEND_IP_ADDRESS}', BACKEND_PORT:'${REACT_APP_BACKEND_PORT}'} " > ${1}config_backend.js
cat ${1}config_backend.js
# shift 1 removes $1 parameter so exec "$@" will execute remaining params if any ie those specified in dockerfile CMD
shift 1
exec "$@"
