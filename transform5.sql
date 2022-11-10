CREATE EXTERNAL TABLE IF NOT EXISTS result_mapreduce
(
    year       STRING,
    month      STRING,
    location   INT,
    pass_count INT
)
    COMMENT 'output3_pass_count'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '$RESULT_MAP_REDUCE_PATH' INTO TABLE result_mapreduce;

CREATE EXTERNAL TABLE IF NOT EXISTS taxi_zone
(
    LocationID   INT,
    Borough      STRING,
    Zone         STRING,
    service_zone STRING
)
    COMMENT 'output3_pass_count'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '$INPUT_DATA_5' INTO TABLE taxi_zone;

CREATE TABLE IF NOT EXISTS taxi_zone_orc
(
    LocationID   INT,
    Borough      STRING,
    Zone         STRING,
    service_zone STRING
)
    COMMENT 'taxi_orc'
    STORED as ORC;

INSERT OVERWRITE TABLE taxi_zone_orc
SELECT *
FROM taxi_zone
WHERE locationid IS NOT NULL;



select *
from (
         select map_result.year,
                map_result.month,
                taxi.borough,
                taxi.zone,
                map_result.pass_count,
                row_number() over (partition by map_result.year,map_result.month order by map_result.pass_count desc) as bourough_rank
         from result_mapreduce map_result
                  join taxi_zone_orc taxi on taxi.locationid = map_result.location
     ) ranks
where bourough_rank <= 3;CREATE EXTERNAL TABLE IF NOT EXISTS result_mapreduce
