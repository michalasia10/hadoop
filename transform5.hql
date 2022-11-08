CREATE EXTERNAL TABLE IF NOT EXISTS result_mapreduce (
year STRING,
month STRING,
location INT,
pass_count INT)
COMMENT 'output3_pass_count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/user/michal_lasia3/map_result';


CREATE EXTERNAL TABLE IF NOT EXISTS taxi_zone (
LocationID INT,
Borough STRING,
Zone STRING,
service_zone STRING)
COMMENT 'output3_pass_count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/michal_lasia3/taxi_zone';

CREATE TABLE IF NOT EXISTS taxi_zone_orc (
LocationID INT,
Borough STRING,
Zone STRING,
service_zone STRING)
COMMENT 'taxi_orc'
STORED as ORC;

INSERT OVERWRITE TABLE taxi_zone_orc
    SELECT * FROM taxi_zone WHERE locationid IS NOT NULL;


#### IDEAS

https://stackoverflow.com/questions/16720525/how-to-select-top-3-values-from-each-group-in-a-table-with-sql-which-have-duplic

https://stackoverflow.com/questions/176964/select-top-10-records-for-each-category

https://stackoverflow.com/questions/16560770/sql-select-top-5-every-month

# clever http://www.silota.com/docs/recipes/sql-top-n-group.html
