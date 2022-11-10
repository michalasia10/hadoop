#!/bin/bash
# shellcheck disable=SC2164
cd

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

echo ">>>> Tests for file: START"
MAPPER_FILE="mapper.py"
REDUCER_FILE="reducer.py"
HIVE_FILE="transform5.hql"
test -f $MAPPER_FILE && echo "$MAPPER_FILE was uploaded. OK"
test -f $REDUCER_FILE && echo "$REDUCER_FILE was uploaded. OK"
test -f $HIVE_FILE && echo "$HIVE_FILE was uploaded. OK"

echo "Provide link to folder in bucket or use default; [ENTER or PROVIDE YOUR BUCKET ]"
read -e -p "bucket link: " -i "moj_bucket" BUCKET
echo "You're going to be use: "$BUCKET

echo " "
echo ">>>> copying all data and scripts from bucket to local : START"
hadoop fs -copyToLocal $BUCKET/*
echo " "
echo ">>>> copying all data and scripts from bucket to local : DONE"

echo " "
echo ">>>> creating folders for inputs: START"
HIVE_PATH="hive/"
hadoop fs -mkdir -p project/hadoop/mapreduce/input
hadoop fs -mkdir -p project/haddop/hive/input
mkdir -p $HIVE_PATH
echo " "
echo ">>>> creating folders for inputs: DONE"

echo " "
echo ">>> injecting path to hdfs file"
TRANSFORMED_HIVE=transform5_with_paths.hql
cat transform5.hql | sed "s/\$RESULT_MAP_REDUCE_PATH/essa/g" > $TRANSFORMED_HIVE



echo " "
echo ">>>> copying scripts and data that needs to be available in HDFS to launch this script: START"
hadoop fs -copyFromLocal yellow_tripdata_2018-11.csv project/hadoop/mapreduce/input
hadoop fs -copyFromLocal yellow_tripdata_2018-12.csv project/hadoop/mapreduce/input
cp *.hql $HIVE_PATH
echo " "
echo ">>>> copying scripts and data that needs to be available in HDFS to launch this script: DONE"

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

#
#echo " "
#echo ">>>> launching the Hive/Pig script - processing (5)"
## TODO: please customize the command below so that it run Hive or Pig script (5)
## example for Hive
hive -f transform5.hql
## example for Pig
#pig -f transform5.pig
#
#echo " "
#echo ">>>> getting the final result (6) from HDFS to the local file system"
#mkdir -p ./output6
#hadoop fs -copyToLocal output6/* ./output6
#
#echo " "
#echo " "
#echo " "
#echo " "
#echo ">>>> presenting the obtained the final result (6)"
#cat ./output6/*
