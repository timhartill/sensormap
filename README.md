# SensorMap Application

For screenshot and link to download demo json playback data go to bottom of page at https://www.neuralition.com

This application displays detected objects in realtime on a map. 

Typically inputs would based on json output from performing realtime object detection on multiple video camera input streams but here we also provide a player application that will stream pre-recorded json. Aside from video streams this app could also be used to display events from IoT streams or any other sensor type where displaying the result on a geographical map makes sense.


![UI](readme-images/sensormap_full.png?raw=true "UI")

This document describes the full end to end SensorMap application that is a heavily modified version of the Nvidia DeepStream 360 application (https://github.com/NVIDIA-AI-IOT/deepstream_360_d_smart_parking_application.git). Major changes include:

- Mocks up a demo "Campus" based on multi-camera video outputs from the Virat dataset (http://www.viratdata.org). Note that the mapping between the Virat camera views and the geographical map is very approximate and just for demonstration purposes!
- Made generic to work with multiple classes of objects.
- Displays different classes of objects with different colours/styles.
- Removed Apache Spark module in favour of a python 'processor' module to facilitate running in smaller memory footprint.
- Added a python 'player' application that writes recorded json into the raw queue to facilitate testing in environments without access to a gpu.


## Getting Started

To get started, clone this repository. 

The directory structure is:

1. [\analytics_server_docker](https://github.com/timhartill/sensormap/tree/master/analytics_server_docker) - the dockerized sensorMap application
2. [\apis](https://github.com/timhartill/sensormap/tree/master/apis) - source code for the backend node.js application that reads from the cassandra database and elasticsearch and provides information to the ui. 
3. [\ui](https://github.com/timhartill/sensormap/tree/master/ui) - source code for the react-based visualization application
4. [\player](https://github.com/timhartill/sensormap/tree/master/player) - source code for the demo application that "plays" example json messages into the apache kafka raw queue
5. [\tracker](https://github.com/timhartill/sensormap/tree/master/tracker) - source code for the multicamera tracking application that consolidates object detections across cameras, handles overlapping fields of view etc
6. [\processor](https://github.com/timhartill/sensormap/tree/master/processor) - source code for application that writes detections into the cassandra database and calculates anomalies over time which are written to the kafka anomaly topic


To run this application, the user needs to start the following applications in this order:

1. **[Analytics Server](https://github.com/timhartill/sensormap/tree/master/analytics_server_docker)**: Check the README inside the `analytics_server_docker` directory and follow the steps to start the SensorMap docker containers.
2. **[Player](https://github.com/timhartill/sensormap/tree/master/player)**: Check the README inside the `player` directory and follow the steps to start the player once SensorMap is started.

