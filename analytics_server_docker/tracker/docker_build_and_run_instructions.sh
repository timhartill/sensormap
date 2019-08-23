# Build an image from dockerfile in current directory and run it
# if confidence in container build ok is high can go straight to step 3...

# 1. Build the image
docker build --tag=tracker .

# 2. test run image - will fail after being unable to connect to kafka but indicates whether container has been built ok:
docker run --rm tracker

# 3. now must delete old docker-compose container for processor (which is separate from standalone processor container...). Otherwise docker-compose will continue to run with the PREVIOUS processor image!
docker rmi --force analytics_server_docker_mctracker:latest

# 4. run sudo -E docker-compose up -d to test. Then do docker ps and ensure all containers running. 
./start.sh

# 4a. If not all ok:

# 4a1. find container name
docker ps -a

# 4a2. look inside a stopped container
docker start -i <container name>
docker exec -it <container name> /bin/bash 
