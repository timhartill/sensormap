# Streaming Player

# Table of contents
1. [Introduction](#introduction)
2. [Configuration](#configuration)
3. [Running Player](#running-player)

# Introduction

This module "plays" a json file into the metromind-raw queue (or other queue as specified in config file). Input file should be of form: 

{json 1}
{json 2}
...
{json n}


# Configuration
The Processor takes in two config files:
1. *Stream Config file:* This file describes the config needed for input/output of player. Example stream config


2. *Player Config file:* This file describes the configuration parameters for the player.

# Running Player

Run the following commands:
1. cd /sensormap/player/usecasecode/player

2. Download json file from https://drive.google.com/file/d/1YYoDzcDVQOd0cSrr6mGk5N16mfet9R99/view?usp=sharing and put into the /json_playback subdirectory

3. Update /config/config_player_stream.json:
            "inputFileConfig": {"inputDir":"/your/directory/gitrepos/sensormap/player/json_playback",
                                "inputFile":"your downloaded json file.json"}

4. python stream_player.py --sconfig=`<path to stream config file>` --config=`<path to processor config file>`



