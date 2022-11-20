from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG(
        "project1-workflow",
        start_date=datetime(2015, 12, 1),
        schedule_interval=None,

        params={
            "dags_home": Param(default="/home/DAG_DIRECTORY/airflow/dags", description="Path to dag directory",
                               type="string"),
            "input_dir": Param(default="gs://BUCKET/project1/input", description="Link to your bucket", type="string"),
            "mapreduce_output": Param(default="/project/mapreduce/output", type="string"),
            "mapreduce_input_hdfs": Param(default="/project/mapreduce/input", type="string"),
            "final_output_file_name": Param(default="final_output.csv", type="string", format="*.csv"),
            "classic_or_streaming": Param(default="streaming", enum=["classic", "streaming"]),
            "mapper_file": Param(default="mapper.py"),
            "reducer_file": Param(default="reducer.py"),
            "pig_or_hive": Param(default="hive", enum=["hive", "pig"]),
            "mapreduce_merged_file_name": Param(default="mapreduce.csv"),
            "output6_table_name":Param(default="output2"),
            "output6_hdfs":Param(default="/project/hive/output/")
        },
        render_template_as_native_obj=True
) as dag:
    clean_mapreduce_input = BashOperator(
        task_id="mapreduce_input",
        bash_command="""if $(hadoop fs -test -d {{ params.mapreduce_input_hdfs }}) ; then hadoop fs -rm -f -r {{ params.mapreduce_input_hdfs }}; fi""",
    )
    clean_mapreduce_output = BashOperator(
        task_id="mapreduce_output",
        bash_command="""if $(hadoop fs -test -d {{ params.mapreduce_output }}) ; then hadoop fs -rm -f -r {{ params.mapreduce_output }}; fi""",
    )
    clean_final_output = BashOperator(
        task_id="clean_final_output",
        bash_command="""if $(test -f {{ params.final_output_file_name }}) ; then rm {{ params.final_output_file_name }}; fi""",
    )
    drop_tables = BashOperator(
        task_id="drop_tables",
        bash_command="""hive -e 'drop table if exists result_mapreduce'
                        hive -e 'drop table if exists taxi_zone'
                        hive -e 'drop table if exists taxi_zone_orc'
                        hive -e 'drop table if exists {{ params.output6_table_name }}'
                        hive -e 'drop table if exists final_result_orc'
        """,
    )

    check_if_mapper_exist = BashOperator(
        task_id="check_if_mapper_exist",
        bash_command=""" if $(test -f {{ params.mapper_file }}) ;then echo 'File :{{ params.mapper_file }} exists. OK';fi """
    )
    check_if_reducer_exist = BashOperator(
        task_id="check_if_reducer_exist",
        bash_command=""" if $(test -f {{ params.reducer_file }}) ;then echo 'File :{{ params.reducer_file }} exists. OK';fi """
    )


    def _pick_classic_or_streaming():
        if dag.params['classic_or_streaming'] == "classic":
            return "mapreduce_classic"
        else:
            return "hadoop_streaming"


    pick_classic_or_streaming = BranchPythonOperator(
        task_id="pick_classic_or_streaming", python_callable=_pick_classic_or_streaming
    )

    mapreduce_classic = BashOperator(
        task_id="mapreduce_classic",
        bash_command=""" echo 'This pipeline doesn't support mapreduce classic.' """,
    )
    hdfs_mkdir_map_reduce_input = BashOperator(
        task_id="hdfs_mkdir_map_reduce_input",
        bash_command=""" hadoop fs -mkdir -p {{ params.mapreduce_input_hdfs }}"""
    )

    get_and_merge_map_reduce_output = BashOperator(
        task_id='get_and_merge_map_reduce_output',
        bash_command="""hdfs dfs -getmerge {{ params.mapreduce_output }}/* {{ params.mapreduce_merged_file_name }}
         hadoop fs -mkdir -p {{ params.mapreduce_output }}
         hadoop fs -copyFromLocal {{ params.mapreduce_merged_file_name }} {{ params.mapreduce_output }} """,
        trigger_rule="one_success"
    )

    hadoop_streaming = BashOperator(
        task_id="hadoop_streaming",
        bash_command="""mapred streaming \
-files {{ params.dags_home }}/project_files/{{ params.mapper_file }},\
{{ params.dags_home }}/project_files/{{ params.reducer_file }} \
-input {{ params.input_dir }}/datasource1 \
-mapper {{ params.mapper_file }} \
-reducer {{ params.reducer_file }} \
-output {{ params.mapreduce_output }}""",
    )


    def _pick_pig_or_hive():
        if dag.params['pig_or_hive'] == "pig":
            return "pig"
        else:
            return "hive"


    pick_pig_or_hive = BranchPythonOperator(
        task_id="pick_pig_or_hive", python_callable=_pick_pig_or_hive,

    )

    hive = BashOperator(
        task_id="hive",
        bash_command="""hive -f {{ params.dags_home }}/project_files/transform5.hql \
      -hiveconf RESULT_MAP_REDUCE_PATH={{ params.mapreduce_output }}/{{ params.mapreduce_merged_file_name }} \
      -hiveconf USED_HDFS_HIVE_INPUT_PATH={{ params.input_dir }}/datasource4/ \
      -hiveconf USED_OUTPUT_TABLE_NAME={{ params.output6_table_name }} \
      -hiveconf USED_OUTPUT_PATH={{ params.output6_hdfs }} \
      """, trigger_rule="one_success"

    )

    pig = BashOperator(
        task_id="pig",
        bash_command=""" echo 'This pipeline doesn't support pig.' """, trigger_rule="one_success"

    )

    get_output = BashOperator(
        task_id="get_output",
        bash_command="""hadoop fs -test -d {{ params.output6_hdfs }}
                        hadoop dfs -getmerge {{ params.output6_hdfs }} {{ params.final_output_file_name }}
                        echo '   '
                        echo 'Year,Month,Borough,Zone,Passengers_count,Borough_Rank,'
                        echo '   '
                        cat {{ params.final_output_file_name }}""", trigger_rule="one_success"
    )

    [clean_mapreduce_input, clean_mapreduce_output, clean_final_output, clean_mapreduce_input, drop_tables,
     check_if_mapper_exist,
     check_if_reducer_exist] >> hdfs_mkdir_map_reduce_input
    hdfs_mkdir_map_reduce_input >> pick_classic_or_streaming
    pick_classic_or_streaming >> [mapreduce_classic, hadoop_streaming]
    [mapreduce_classic, hadoop_streaming] >> get_and_merge_map_reduce_output
    get_and_merge_map_reduce_output >> pick_pig_or_hive
    pick_pig_or_hive >> [pig, hive]
    [pig, hive] >> get_output
