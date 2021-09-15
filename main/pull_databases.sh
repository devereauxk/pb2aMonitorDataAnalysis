#!/bin/sh

RECEPTION_DIR=~/Desktop/polarbear/databases/current/
SSH_LOGIN=kdevereaux@tostada1.physics.berkeley.edu:

# directories in tostada to find files
MONITOR_DIR=/home/tadkins/SimonsArray/databases/
DATA_QUALITY_DIR=/home/ktcrowley/sa_database/
RUNID_DIR=/data/pb2/ChileData/databases/
APEX_DIR=$MONITOR_DIR

# file names of desired files in tostada
MONITOR=pb2a_monitor.db-20210601
DATA_QUALITY=data_quality_output_pb2a_v3.db
RUNID=pb2a_runid.db
APEX=pb2a_apex.db-20210322

# downloading desired files
scp $SSH_LOGIN$MONITOR_DIR$MONITOR $RECEPTION_DIR
scp $SSH_LOGIN$DATA_QUALITY_DIR$DATA_QUALITY $RECEPTION_DIR
scp $SSH_LOGIN$RUNID_DIR$RUNID $RECEPTION_DIR
scp $SSH_LOGIN$APEX_DIR$APEX $RECEPTION_DIR

# renaming files on local machine
mv $RECEPTION_DIR$MONITOR $RECEPTION_DIR"pb2a_monitor.db"
mv $RECEPTION_DIR$DATA_QUALITY $RECEPTION_DIR"pb2a_data_quality.db"
mv $RECEPTION_DIR$RUNID $RECEPTION_DIR"pb2a_runid.db"
mv $RECEPTION_DIR$APEX $RECEPTION_DIR"pb2a_apex.db"
