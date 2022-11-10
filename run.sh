#!/bin/bash
# shellcheck disable=SC2164
cd

#CONSTs
MAPPER_FILE="mapper.py"
REDUCER_FILE="reducer.py"
HIVE_FILE="transform5.hql"
HIVE_PATH="hive/"
TRANSFORMED_HIVE="transform5_with_paths.hql"
HDFS_MAPREDUCE_INPUT_PATH="project/hadoop/mapreduce/input"
DEFAULT_BUCKET="gs://ml"

### CLEANING
echo " "
echo ">>>> removing leftovers from previous launches"
##delete the output directory for mapreduce job (3)
#if $(hadoop fs -test -d ./output_mr3) ; then hadoop fs -rm -f -r ./output_mr3; fi
## delete the output directory for the final project result (6)
#if $(hadoop fs -test -d ./output6) ; then hadoop fs -rm -f -r ./output6; fi
## delete the directory with the other project's files (scripts, jar files and everything that needs to be available in HDFS to launch this script)
#if $(hadoop fs -test -d ./project_files) ; then hadoop fs -rm -f -r ./project_files; fi
## remove the local output directory containing the final result of the project (6)
#if $(test -d ./output6) ; then rm -rf ./output6; fi


### TESTS
echo ">>>> Tests for files: START"

test -f $MAPPER_FILE && echo "$MAPPER_FILE was uploaded. OK"
test -f $REDUCER_FILE && echo "$REDUCER_FILE was uploaded. OK"
test -f $HIVE_FILE && echo "$HIVE_FILE was uploaded. OK"

### BUCKET READER
echo "Provide link to folder in bucket or use default; [ENTER or PROVIDE YOUR BUCKET ]"
read -e -p "bucket link: " -i $DEFAULT_BUCKET BUCKET
echo "You're going to be use: $BUCKET"

### DOWNLOAD DATA FROM BUCKET
echo " "
echo ">>>> copying all data and scripts from bucket to local : START"
hadoop fs -copyToLocal $BUCKET/*
echo " "
echo ">>>> copying all data and scripts from bucket to local : DONE"

### PREPARE FOLDERS FOR INPUTS
echo " "
echo ">>>> creating folders for inputs: START"
hadoop fs -mkdir -p $HDFS_MAPREDUCE_INPUT_PATH
mkdir -p $HIVE_PATH
echo " "
echo ">>>> creating folders for inputs: DONE"

### INJECTING PATH TO HIVE
echo " "
echo ">>> injecting path to hdfs file"
cat transform5.hql | sed "s/\$RESULT_MAP_REDUCE_PATH/essa/g" > $TRANSFORMED_HIVE


### COPY DATA FOR MAPREDUCE/ HADOOP STREAMING TO HDFS STORAGE
echo " "
echo ">>>> copying scripts and data that needs to be available in HDFS to launch this script: START"
hadoop fs -copyFromLocal yellow_tripdata_2018-11.csv project/hadoop/mapreduce/input
hadoop fs -copyFromLocal yellow_tripdata_2018-12.csv project/hadoop/mapreduce/input
cp *.hql $HIVE_PATH
echo " "
echo ">>>> copying scripts and data that needs to be available in HDFS to launch this script: DONE"

### LAUNCH HADOOP STREAMING
echo " "
echo ">>>> launching the MapReduce job as Hadoop Streaming- processing (2): START"
mapred streaming \
  -files $MAPPER_FILE,$REDUCER_FILE \
  -input project/hadoop/mapreduce/input/*.csv \
  -output project/hadoop/mapreduce/output \
  -mapper $MAPPER_FILE \
  -reducer $REDUCER_FILE
echo " "
echo ">>>> launching the MapReduce job as Hadoop Streaming- processing (2): DONE"


### LAUNCH HIVE
#
#echo " "
#echo ">>>> launching the Hive/Pig script - processing (5)"
## TODO: please customize the command below so that it run Hive or Pig script (5)
## example for Hive
hive -f transform5.hql
## example for Pig


### COPY FINAL OUTPUT TO LOCAL STORAGE

#
#echo " "
#echo ">>>> getting the final result (6) from HDFS to the local file system"
#mkdir -p ./output6
#hadoop fs -copyToLocal output6/* ./output6

### PRESENT FINAL OUTPUT

#echo " "
#echo " "
#echo " "
#echo " "
#echo ">>>> presenting the obtained the final result (6)"
#cat ./output6/*
