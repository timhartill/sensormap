# Streaming Processor

# Table of contents
1. [Introduction](#introduction)
2. [System Overview](#system-overview)
3. [Configuration](#configuration)
4. [Running Processor](#running-tracker)

# Introduction

Replacement for the Apache Spark stream processing anomaly module in the original Deepstream 360 application which consumed too much memory to run well on small laptops.

This module reads the metromind-start queue and writes out to the anomaly queue as well as the Cassandra database that in turn is read by the apis and ui application.

# System Overview


# Configuration
The Processor takes in two config files:
1. *Stream Config file:* This file describes the config needed for input/output of tracker. Example stream config

    ```json
    {
        "profileTime": false,
        "msgBrokerConfig": {
            "inputKafkaServerUrl": "kafka:9092",
            "inputKafkaTopic": "metromind-start",
            "outputKafkaServerUrl": "kafka:9092",
            "outputKafkaTopic": "metromind-anomaly"
        }
    }
    ```
2. *Processor Config file:* This file describes the configuration parameters for the processor.

# Running Processor
We assume that `processor_dir` corresponds to the base processor directory (e.g. `/home/user/git/processor`).

Run the following commands:

```bash
<machine>$ python usecasecode/processor/stream_process.py --sconfig=`<path to stream config file>` --config=`<path to processor config file>'
...
```


