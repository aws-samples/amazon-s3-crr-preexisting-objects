#!/bin/bash

# $1 is the S3 location of copy_objects.py,
# which is passed as an arugment in launch_emr.sh. 
aws s3 cp $1 /home/hadoop/
