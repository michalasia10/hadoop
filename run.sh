#!/bin/bash
# shellcheck disable=SC2164

#CONSTs
MAPPER_FILE="mapper.py"
REDUCER_FILE="reducer.py"
SCRIPT_HIVE_FILE="transform5.hql"
TRANSFORMED_HIVE="transform5_with_paths.hql"
TRANSFORMED_HIVE1="transform5_with_paths1.hql"
DEFAULT_HDFS_MAPREDUCE_INPUT_PATH="project/hadoop/mapreduce/input"
DEFAULT_HDFS_MAPREDUCE_OUTPUT_PATH="project/hadoop/mapreduce/output"
DEFAULT_HDFS_HIVE_INPUT_PATH="project/hadoop/pig/input"
DEFAULT_BUCKET="gs://wsb-pbl-ml-bucket/project/"
DEFAULT_DATA_SOURCE_MAP_REDUCE="input/datasource1"
DEFAULT_DATA_SOURCE_HIVE="input/datasource4"
DEFAULT_HDFS_MAPREDUCE_OUTPUT_FILE="output_file.csv"

function test_if_file_exist() {
  if [ -f ${1} ]; then
    echo "${1} was uploaded. OK"
  else
    echo "File: ${1} not found. Pls upload file: ${1}. Script will shutdown"
    exit
  fi
}

function test_if_directory_exist() {
  if [ -d ${1} ]; then
    echo "${1} found. OK"
  else
    echo "Catalog/folder: ${1} not found. not found. Script will shutdown"
    exit
  fi
}

### CLEANING
echo " "
echo ">>>> removing leftovers from previous launches"
##delete the output directory for mapreduce job (3)
if $(hadoop fs -test -d ./$DEFAULT_HDFS_MAPREDUCE_OUTPUT_PATH); then hadoop fs -rm -f -r ./$DEFAULT_HDFS_MAPREDUCE_OUTPUT_PATH; fi
if $(hadoop fs -test -d ./$DEFAULT_HDFS_MAPREDUCE_INPUT_PATH); then hadoop fs -rm -f -r ./$DEFAULT_HDFS_MAPREDUCE_INPUT_PATH; fi
if $(hadoop fs -test -d ./$DEFAULT_HDFS_HDFS_HIVE_INPUT_PATH); then hadoop fs -rm -f -r ./$DEFAULT_HDFS_HIVE_INPUT_PATH; fi


### TESTS
echo ">>>> Tests for uploaded files: START"
echo " "
test_if_file_exist $MAPPER_FILE
test_if_file_exist $REDUCER_FILE
test_if_file_exist $SCRIPT_HIVE_FILE
echo " "
echo ">>>> Tests for uploaded files: DONE"
chmod +x *.py

### BUCKET READER
echo " "
read -p "Provide link to folder in bucket or use default; [ENTER to use default ]" BUCKET
USED_BUCKET=${BUCKET:-${DEFAULT_BUCKET}}
echo "You're going to be use: $USED_BUCKET"

### DOWNLOAD DATA FROM BUCKET
echo " "
echo ">>>> copying all data and scripts from bucket to local : START"
hadoop fs -copyToLocal $USED_BUCKET/*
echo " "
echo ">>>> copying all data and scripts from bucket to local : DONE"

### ASK FOR DATASOURCE CATALOGS
echo " "
read -p "Provide catalog name of datasource for mapreduce or use default; [ENTER to use default ]" DATA_SCOURCE_MAP_REDUCE
USED_DATA_SOURCE_MAP_REDUCE=${DATA_SCOURCE_MAP_REDUCE:-${DEFAULT_DATA_SOURCE_MAP_REDUCE}}
echo "You're going to be use: $USED_DATA_SOURCE_MAP_REDUCE"
echo " "
echo ">>>> Test if directory exists: START"
test_if_directory_exist $USED_DATA_SOURCE_MAP_REDUCE
echo ">>>> Test if directory exists: DONE"
echo " "
read -p "Provide catalog name of datasource for hive or use default; [ENTER to use default ]" DATA_SOURCE_HIVE
USED_DATA_SOURCE_HIVE=${DATA_SOURCE_HIVE:-${DEFAULT_DATA_SOURCE_HIVE}}
echo "You're going to be use:" $USED_DATA_SOURCE_HIVE
echo " "
echo ">>>> Test if directory exists: START"
test_if_directory_exist $USED_DATA_SOURCE_HIVE
echo ">>>> Test if directory exists: DONE"
echo " "

### ASK FOR PATH FOR MAPREDUCE INPUT IN HDFS
read -p "Provide path where to put inputs in HDFS for mapreduce or use default; [ENTER to use default ]" HDFS_MAPREDUCE_INPUT_PATH
USED_HDFS_MAPREDUCE_INPUT_PATH=${HDFS_MAPREDUCE_INPUT_PATH:-${DEFAULT_HDFS_MAPREDUCE_INPUT_PATH}}
echo "You're going to be use: $USED_HDFS_MAPREDUCE_INPUT_PATH"
echo " "
### ASK FOR PATH FOR MAPREDUCE OUTPUT IN HDFS
read -p "Provide path where to put output in HDFS for mapreduce or use default; [ENTER to use default ]" HDFS_MAPREDUCE_OUTPUT_PATH
USED_HDFS_MAPREDUCE_OUTPUT_PATH=${HDFS_MAPREDUCE_OUTPUT_PATH:-${DEFAULT_HDFS_MAPREDUCE_OUTPUT_PATH}}
echo "You're going to be use: $USED_HDFS_MAPREDUCE_OUTPUT_PATH"
echo " "

### ASK FOR OUTPUT NAME FROM MAPREDUCE
read -p "Provide name for output file for mapreduce result or use default; [ENTER to use default ]" HDFS_MAPREDUCE_OUTPUT_FILE
USED_HDFS_MAPREDUCE_OUTPUT_FILE=${HDFS_MAPREDUCE_OUTPUT_FILE:-${DEFAULT_HDFS_MAPREDUCE_OUTPUT_FILE}}
echo "You're going to be use: $USED_HDFS_MAPREDUCE_OUTPUT_FILE"
echo " "
### ASK FOR PATH WHERE TO PUT INPUTS FOR HIVE
read -p "Provide path where to put inputs in HDFS for hive or use default; [ENTER to use default ]" HDFS_HIVE_INPUT_PATH
USED_HDFS_HIVE_INPUT_PATH=${HDFS_HIVE_INPUT_PATH:-${DEFAULT_HDFS_HIVE_INPUT_PATH}}
echo "You're going to be use: $USED_HDFS_HIVE_INPUT_PATH"
echo " "

### PREPARE FOLDERS FOR INPUTS
echo " "
echo ">>>> creating folders for inputs: START"
hadoop fs -mkdir -p $USED_HDFS_MAPREDUCE_INPUT_PATH
hadoop fs -mkdir -p $USED_HDFS_HIVE_INPUT_PATH
echo " "
echo ">>>> creating folders for inputs: DONE"

### COPY DATA FOR MAPREDUCE/ HADOOP STREAMING TO HDFS STORAGE AND HIVE TO CATALOG FOR INPUT HIVE
echo " "
echo ">>>> copying scripts and data ( HIVE / HDFS / MAPREDUCE ): START"
hadoop fs -copyFromLocal $USED_DATA_SOURCE_MAP_REDUCE/*.csv $USED_HDFS_MAPREDUCE_INPUT_PATH
hadoop fs -copyFromLocal $USED_DATA_SOURCE_HIVE/*.csv $USED_DEFAULT_HDFS_HIVE_INPUT_PATH
echo " "
echo ">>>> copying scripts and data ( HIVE / HDFS / MAPREDUCE ): DONE"

### INJECTING PATH TO HIVE
echo " "
echo ">>> injecting path to hdfs file: START"
RESULT_MAPREDUCE="$USED_HDFS_MAPREDUCE_OUTPUT_FILE/$USED_HDFS_MAPREDUCE_OUTPUT_FILE"
cat $SCRIPT_HIVE_FILE | sed "s|\$RESULT_MAP_REDUCE_PATH|$RESULT_MAPREDUCE|g" >$TRANSFORMED_HIVE
cat $TRANSFORMED_HIVE | sed "s|\$RESULT_MAP_REDUCE_PATH|$USED_HDFS_HIVE_INPUT_PATH|g" >$TRANSFORMED_HIVE1
echo " "
echo ">>> injecting path to hdfs file: DONE"

### LAUNCH HADOOP STREAMING
echo " "
echo ">>>> launching the MapReduce job as Hadoop Streaming- processing (2): START"
mapred streaming \
  -files $MAPPER_FILE,$REDUCER_FILE \
  -input $USED_HDFS_MAPREDUCE_INPUT_PATH/*.csv \
  -output $USED_HDFS_MAPREDUCE_OUTPUT_PATH \
  -mapper $MAPPER_FILE \
  -reducer $REDUCER_FILE
echo " "
echo ">>>> launching the MapReduce job as Hadoop Streaming- processing (2): DONE"
echo " "

### MERGE OUTPUT TO CSV
echo " "
echo ">>>> launch hdfs dfs -getmerge ( merge output from mapreduce ) and copyFromLocal to HDFS for HIVE purpose: START"
hdfs dfs -getmerge $USED_HDFS_MAPREDUCE_OUTPUT_PATH/* $USED_HDFS_MAPREDUCE_OUTPUT_FILE
hadoop fs -copyFromLocal $USED_HDFS_MAPREDUCE_OUTPUT_FILE $USED_HDFS_HIVE_INPUT_PATH
echo ">>>> launch hdfs dfs -getmerge ( merge output from mapreduce ) and copyFromLocal to HDFS for HIVE purpose: DONE"

### LAUNCH HIVE

echo " "
echo ">>>> launching the Hive - processing (5): START"
hive -f $TRANSFORMED_HIVE1
echo " "
echo ">>>> launching the Hive - processing (5): START"
echo " "
echo ">>>> store output from hive: START"
hive -e 'select * from final_result_orc' > final_result.csv
echo " "
echo ">>>> store output from hive: DONE"

## PRESENT FINAL OUTPUT

echo " "
echo " "
echo " "
echo " "
echo ">>>> presenting the obtained the final result (6)"
cat final_result.csv
