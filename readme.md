# Overview

This project is the entry point for an end-to-end data pipeline. It has four main components.

1. Discord Bot
    a. API Connection for
    b. Self-serve Analytics Bot
2. Bigquery
3. DBT
4. BI Presentation Layer (TBD)

The entry point into this pipeline is the discord API. To access this I've used discord.py, a python wrapper. discord.py also doubles to as our means to create an interactive bot on the server, serving up curated metrics on demand.

Bigquery is used as our data warehouse with DBT implementing out transformations.

Finally, a presentation layer will eventually be created. This will tentatively use looker.

[Pipeline Dataflow](/asssets/pipeline_dataflow.png)

## Features

The bot has a number of currently released and planned commands.

### Current Features

+ /topchannels
    - Lists the users top 10 used text channels by number of messages ordered by number of messages in those channels
    - By default topchannels gives an all-time value, optionally the number of days to look back can be specified
+ /topwords
    - Lists the users top 10 words based off of messages from all channels in the server
    - By default topwords uses all messages in the discord, optionally the number of days to look back can be specified

### Planned Changes

+ Add voice activity commands
+ Improve topwords to only display most novel top 10 words
+ Add optional channel argument to topwords
+ Add command to show channels top posters

## Datamarts

Currently only one datamart is available - voice activity. This datamart hosts information on which server members were active in what channels at what times.

[Voice Activity ERD](/assests/voice_activity_erc.png)