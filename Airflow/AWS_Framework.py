from airflow import DAG
import boto3
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.hooks.base import BaseHook
import redshift_connector
from datetime import datetime, timedelta
import json
import psycopg2
import awswrangler as wr
import time

AIRFLOW_DB_NAME = 'airflow'
FORUM_DB_NAME = 'ransomeware'
CREDS_DB_NAME = 'creds'
RS_HOST = 'data-warehouse.czf0ltb1d6ac.us-east-1.redshift.amazonaws.com'
RS_PORT = '5439'
# S3_EXPORT_PATH = 's3://i471-dev-creds-elasticsearch-data/in_progress'
PROTECTION_TIME_GAP_IN_SECONDS = 600
OS_HOST = 'vpc-merlin-h2p2ka4jduxskhb4fjetbznvbi.us-east-1.es.amazonaws.com'

psyclient = boto3.client('redshift')

airflow_db_config = psyclient.get_cluster_credentials(
    DbUser='airflow',
    DbName='airflow',
    ClusterIdentifier='data-warehouse',
    AutoCreate=False  # ,
    # DbGroups=['merlin']
)

forum_db_config = psyclient.get_cluster_credentials(
    DbUser='airflow',
    DbName='ransomeware',
    ClusterIdentifier='data-warehouse',
    AutoCreate=False  # ,
    # DbGroups=['merlin']
)

creds_db_config = psyclient.get_cluster_credentials(
    DbUser='creds',
    DbName='creds',
    ClusterIdentifier='data-warehouse',
    AutoCreate=False  # ,
    # DbGroups=['merlin']
)


def pre_batch(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    cursor = __acquire_rs_cursor(airflow_db_config, AIRFLOW_DB_NAME)
    cursor.execute("""call BATCH_CTL_LOGGING('{V_Batch_Name}','PRE');""".format(
        V_Batch_Name=kwargs['dag'].dag_id
    ))
    cursor.execute("""select last_extract_date from ctl_batch_run where batch_name ='{V_Batch_Name}';""".format(
        V_Batch_Name=kwargs['dag'].dag_id))

    last_extract_date = cursor.fetchall()[0][0]
    last_extract_date_unix_millis = int(time.mktime(last_extract_date.timetuple()) * 1000)
    last_extract_date_str = str(last_extract_date)
    cursor.close()

    print('Last extract date is ' + last_extract_date_str)
    print('Last extract date unix millis is ' + str(last_extract_date_unix_millis))
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key="last_extract_date", value=last_extract_date_str)


def __acquire_rs_cursor(creds, dbname):
    redshift_conn = psycopg2.connect(
        host=RS_HOST,
        port=RS_PORT,
        user=creds['DbUser'],
        password=creds['DbPassword'],
        database=dbname
    )
    redshift_conn.autocommit = True
    return redshift_conn.cursor()


def unload_incremental_s3(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    last_extract_date_str = kwargs['ti'].xcom_pull(task_ids='PRE_BATCH_STEP', key='last_extract_date')
    print('Last extract date is ' + last_extract_date_str)

    last_extract_date = datetime.strptime(last_extract_date_str, "%Y-%m-%d %H:%M:%S") if last_extract_date_str.count(
        '.') == 0 else datetime.strptime(last_extract_date_str, "%Y-%m-%d %H:%M:%S.%f")
    export_file_name = last_extract_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    print('Export file name is: ' + export_file_name)

    extract_date_with_gap = last_extract_date - timedelta(seconds=PROTECTION_TIME_GAP_IN_SECONDS)

    # uncomment next line to force reprocessing of all data from beginning
    # extract_date_with_gap = datetime.strptime('2021-12-04 00:00:00', "%Y-%m-%d %H:%M:%S") - timedelta(seconds=PROTECTION_TIME_GAP_IN_SECONDS)

    unload_query = UNLOAD_QUERY.format(
        export_path='s3://' + kwargs["BUCKET"] + '/' + kwargs["PREFIX"],  # S3_EXPORT_PATH,
        mv_name=kwargs["MV_NAME"],
        # lowest_extract_date = extract_date_with_gap,
        export_file_name=export_file_name
    )
    print(unload_query)
    cursor = __acquire_rs_cursor(forum_db_config, FORUM_DB_NAME)
    cursor.execute(unload_query)
    cursor.close()


UNLOAD_QUERY = """
unload (
    $$
	select * from {mv_name}
    $$
)
to '{export_path}{export_file_name}'
iam_role 'arn:aws:iam::352821864386:role/S3ForRedshift' allowoverwrite
parallel off
JSON;
"""


def unload_incremental_s3_creds(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    last_extract_date_str = kwargs['ti'].xcom_pull(task_ids='PRE_BATCH_STEP', key='last_extract_date')
    print('Last extract date is ' + last_extract_date_str)

    last_extract_date = datetime.strptime(last_extract_date_str, "%Y-%m-%d %H:%M:%S") if last_extract_date_str.count(
        '.') == 0 else datetime.strptime(last_extract_date_str, "%Y-%m-%d %H:%M:%S.%f")
    export_file_name = last_extract_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    print('Export file name is: ' + export_file_name)

    extract_date_with_gap = last_extract_date - timedelta(seconds=PROTECTION_TIME_GAP_IN_SECONDS)

    # uncomment next line to force reprocessing of all data from beginning
    # extract_date_with_gap = datetime.strptime('2021-12-04 00:00:00', "%Y-%m-%d %H:%M:%S") - timedelta(seconds=PROTECTION_TIME_GAP_IN_SECONDS)

    unload_query = UNLOAD_QUERY.format(
        export_path='s3://' + kwargs["BUCKET"] + '/' + kwargs["PREFIX"],  # S3_EXPORT_PATH,
        mv_name=kwargs["MV_NAME"],
        # lowest_extract_date = extract_date_with_gap,
        export_file_name=export_file_name
    )
    print(unload_query)
    cursor = __acquire_rs_cursor(creds_db_config, CREDS_DB_NAME)
    cursor.execute(unload_query)
    cursor.close()


UNLOAD_QUERY = """
unload (
    $$
	select * from {mv_name}
    $$
)
to '{export_path}{export_file_name}'
iam_role 'arn:aws:iam::242276815153:role/RedshiftS3Import' allowoverwrite
parallel off
JSON;
"""


def post_batch(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    cursor = __acquire_rs_cursor(airflow_db_config, AIRFLOW_DB_NAME)
    cursor.execute("""call BATCH_CTL_LOGGING('{V_Batch_Name}','POST');""".format(
        V_Batch_Name=kwargs['dag'].dag_id))
    cursor.close()


def delete_files(**kwargs):
    BUCKET = kwargs["BUCKET"]  # 'ransomeware-data-poc'
    PREFIX = kwargs["PREFIX"]  # 'opensearch_data/in_progress/'
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    files_in_folder = response["Contents"]
    files_to_delete = []
    i = 0
    for f in files_in_folder:
        if i > 0:
            files_to_delete.append({"Key": f["Key"]})
        i = i + 1
    print(len(files_to_delete))
    if len(files_to_delete) > 0:
        response = s3_client.delete_objects(
            Bucket=BUCKET, Delete={"Objects": files_to_delete}
        )


def move_files(**kwargs):
    old_bucket_name = kwargs["BUCKET"]  # 'ransomeware-data-poc'
    old_prefix = kwargs["OLD_PATH"]  # 'opensearch_data/in_progress/'
    new_bucket_name = kwargs["BUCKET"]  # 'ransomeware-data-poc'
    new_prefix = kwargs["NEW_PATH"]  # 'opensearch_data/completed/'
    s3 = boto3.resource('s3')
    old_bucket = s3.Bucket(old_bucket_name)
    new_bucket = s3.Bucket(new_bucket_name)

    for obj in old_bucket.objects.filter(Prefix=old_prefix):
        old_source = {'Bucket': old_bucket_name,
                      'Key': obj.key}
        # replace the prefix
        new_key = obj.key.replace(old_prefix, new_prefix + 'processed', 1)
        new_obj = new_bucket.Object(new_key)
        new_obj.copy(old_source)


def refresh_mv(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    print('Refreshing creds count materialized view...')

    cursor = __acquire_rs_cursor(forum_db_config, FORUM_DB_NAME)
    cursor.execute("REFRESH MATERIALIZED VIEW {};".format(kwargs['MV_NAME']))
    cursor.close()


def refresh_mv_creds(**kwargs):
    print('Context data passed: ')
    print(kwargs)

    print('Refreshing creds count materialized view...')

    cursor = __acquire_rs_cursor(creds_db_config, CREDS_DB_NAME)
    cursor.execute("REFRESH MATERIALIZED VIEW {};".format(kwargs['MV_NAME']))
    cursor.close()


def opensearch_transfer(**kwargs):
    client = wr.opensearch.connect(host=OS_HOST)
    BUCKET = kwargs["BUCKET"]
    PREFIX = kwargs["PREFIX"]
    INDEX = kwargs["INDEX"]  # 'channel'
    INDEX_ID_KEY = kwargs["INDEX_ID_KEY"]  # ["id"] # can be multiple fields. arg applicable to all index_* functions
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    files_in_folder = response["Contents"]
    # print(files_in_folder)
    files_to_process = []
    i = 0
    for f in files_in_folder:
        if i > 0:
            files_to_process.append({"Key": f["Key"]})
    i = i + 1
    # print(files_to_process)

    for to_os in files_to_process:
        print('to process for ---- > s3://' + BUCKET + '/' + to_os['Key'])
        wr.opensearch.index_json(
            client,
            path='s3://' + BUCKET + '/' + to_os['Key'],  # path can be s3 or local
            index=INDEX,
            id_keys=INDEX_ID_KEY)


############################## DAG DEFINITION ##############################
############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 10, 18),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    dag_id='forum_batch_incremental',
    default_args=default_args,
    schedule_interval=None,  # '*/60 * * * *',
    tags=['forum-data'],
    catchup=False,
    max_active_runs=1
)

PRE_BATCH_STEP = PythonOperator(
    task_id="PRE_BATCH_STEP",
    python_callable=pre_batch,
    provide_context=True,
    retries=0, dag=dag
)

################## Topic ################

REFRESH_TOPIC = PythonOperator(
    task_id="REFRESH_TOPIC",
    python_callable=refresh_mv,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_TOPIC'},
    retries=0, dag=dag
)

TOPIC_UNLOAD_S3 = PythonOperator(
    task_id="TOPIC_UNLOAD_S3",
    python_callable=unload_incremental_s3,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'topic/in_progress/', 'MV_NAME': 'MV_TOPIC'},
    retries=0, dag=dag
)

TOPIC_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="TOPIC_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'topic/in_progress/', 'INDEX': 'topic',
               'INDEX_ID_KEY': ["id"]},
    retries=0, dag=dag
)

MOVE_TOPIC_FILES = PythonOperator(
    task_id="MOVE_TOPIC_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'topic/in_progress/', 'NEW_PATH': 'topic/completed/'},
    retries=0, dag=dag
)

DELETE_TOPIC_FILES = PythonOperator(
    task_id="DELETE_TOPIC_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'topic/in_progress/'},
    retries=0, dag=dag
)

################## Channel ################

REFRESH_CHANNEL = PythonOperator(
    task_id="REFRESH_CHANNEL",
    python_callable=refresh_mv,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_CHANNEL'},
    retries=0, dag=dag
)

CHANNEL_UNLOAD_S3 = PythonOperator(
    task_id="CHANNEL_UNLOAD_S3",
    python_callable=unload_incremental_s3,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'channel/in_progress/', 'MV_NAME': 'MV_CHANNEL'},
    retries=0, dag=dag
)

CHANNEL_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="CHANNEL_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'channel/in_progress/', 'INDEX': 'channel',
               'INDEX_ID_KEY': ["id"]},
    retries=0, dag=dag
)

MOVE_CHANNEL_FILES = PythonOperator(
    task_id="MOVE_CHANNEL_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'channel/in_progress/', 'NEW_PATH': 'channel/completed/'},
    retries=0, dag=dag
)

DELETE_CHANNEL_FILES = PythonOperator(
    task_id="DELETE_CHANNEL_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'channel/in_progress/'},
    retries=0, dag=dag
)

################## Message ################

REFRESH_MESSAGE = PythonOperator(
    task_id="REFRESH_MESSAGE",
    python_callable=refresh_mv,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_MESSAGE'},
    retries=0, dag=dag
)

MESSAGE_UNLOAD_S3 = PythonOperator(
    task_id="MESSAGE_UNLOAD_S3",
    python_callable=unload_incremental_s3,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'message/in_progress/', 'MV_NAME': 'MV_MESSAGE'},
    retries=0, dag=dag
)

MESSAGE_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="MESSAGE_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'message/in_progress/', 'INDEX': 'message',
               'INDEX_ID_KEY': ["id"]},
    retries=0, dag=dag
)

MOVE_MESSAGE_FILES = PythonOperator(
    task_id="MOVE_MESSAGE_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'message/in_progress/', 'NEW_PATH': 'message/completed/'},
    retries=0, dag=dag
)

DELETE_MESSAGE_FILES = PythonOperator(
    task_id="DELETE_MESSAGE_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'message/in_progress/'},
    retries=0, dag=dag
)

################## actor_activity ################

REFRESH_ACTOR_ACTIVITY = PythonOperator(
    task_id="REFRESH_ACTOR_ACTIVITY",
    python_callable=refresh_mv,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_HACKERHANDLE_ACTIVITY_METRICS'},
    retries=0, dag=dag
)

ACTOR_ACTIVITY_UNLOAD_S3 = PythonOperator(
    task_id="ACTOR_ACTIVITY_UNLOAD_S3",
    python_callable=unload_incremental_s3,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'actor_activity/in_progress/',
               'MV_NAME': 'MV_HACKERHANDLE_ACTIVITY_METRICS'},
    retries=0, dag=dag
)

ACTOR_ACTIVITY_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="ACTOR_ACTIVITY_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'actor_activity/in_progress/', 'INDEX': 'actor_activity',
               'INDEX_ID_KEY': ["channel_name", "date", "hour", "handle", "threat_actor_id"]},
    retries=0, dag=dag
)

MOVE_ACTOR_ACTIVITY_FILES = PythonOperator(
    task_id="MOVE_ACTOR_ACTIVITY_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'actor_activity/in_progress/',
               'NEW_PATH': 'actor_activity/completed/'},
    retries=0, dag=dag
)

DELETE_ACTOR_ACTIVITY_FILES = PythonOperator(
    task_id="DELETE_ACTOR_ACTIVITY_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'actor_activity/in_progress/'},
    retries=0, dag=dag
)

################## underground_activity ################

REFRESH_UNDERGROUND_ACTIVITY = PythonOperator(
    task_id="REFRESH_UNDERGROUND_ACTIVITY",
    python_callable=refresh_mv,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_FORUMTOPICS_ACTIVITY'},
    retries=0, dag=dag
)

UNDERGROUND_ACTIVITY_UNLOAD_S3 = PythonOperator(
    task_id="UNDERGROUND_ACTIVITY_UNLOAD_S3",
    python_callable=unload_incremental_s3,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'underground_activity/in_progress/',
               'MV_NAME': 'MV_FORUMTOPICS_ACTIVITY'},
    retries=0, dag=dag
)

UNDERGROUND_ACTIVITY_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="UNDERGROUND_ACTIVITY_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'underground_activity/in_progress/',
               'INDEX': 'underground_activity',
               'INDEX_ID_KEY': ["channel_name", "is_author", "topic_title", "handle", "threat_actor_id", "topic_id",
                                "channel_type", "channel_id"]},
    retries=0, dag=dag
)

MOVE_UNDERGROUND_ACTIVITY_FILES = PythonOperator(
    task_id="MOVE_UNDERGROUND_ACTIVITY_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'underground_activity/in_progress/',
               'NEW_PATH': 'underground_activity/completed/'},
    retries=0, dag=dag
)

DELETE_UNDERGROUND_ACTIVITY_FILES = PythonOperator(
    task_id="DELETE_UNDERGROUND_ACTIVITY_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'underground_activity/in_progress/'},
    retries=0, dag=dag
)

################## contact_info ################

REFRESH_CONTACT_INFO = PythonOperator(
    task_id="REFRESH_CONTACT_INFO",
    python_callable=refresh_mv_creds,
    provide_context=True,
    op_kwargs={'MV_NAME': 'MV_CONTACT_INFO'},
    retries=0, dag=dag
)

CONTACT_INFO_UNLOAD_S3 = PythonOperator(
    task_id="CONTACT_INFO_UNLOAD_S3",
    python_callable=unload_incremental_s3_creds,
    provide_context=True,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'contact_info_agg/in_progress/',
               'MV_NAME': 'MV_CONTACT_INFO'},
    retries=0, dag=dag
)

CONTACT_INFO_OPENSEARCH_TRANSFER = PythonOperator(
    task_id="CONTACT_INFO_OPENSEARCH_TRANSFER",
    python_callable=opensearch_transfer,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'contact_info_agg/in_progress/', 'INDEX': 'contact_info',
               'INDEX_ID_KEY': ["id"]},
    retries=0, dag=dag
)

MOVE_CONTACT_INFO_FILES = PythonOperator(
    task_id="MOVE_CONTACT_INFO_FILES",
    python_callable=move_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'OLD_PATH': 'contact_info_agg/in_progress/',
               'NEW_PATH': 'contact_info_agg/completed/'},
    retries=0, dag=dag
)

DELETE_CONTACT_INFO_FILES = PythonOperator(
    task_id="DELETE_CONTACT_INFO_FILES",
    python_callable=delete_files,
    op_kwargs={'BUCKET': 'i471-dev-airflow-forum-data', 'PREFIX': 'contact_info_agg/in_progress/'},
    retries=0, dag=dag
)

POST_BATCH_STEP = PythonOperator(
    task_id="POST_BATCH_STEP",
    python_callable=post_batch,
    trigger_rule='none_failed_or_skipped',
    provide_context=True,
    retries=0, dag=dag)

# DAG order definition:
PRE_BATCH_STEP >> [REFRESH_TOPIC, REFRESH_CHANNEL, REFRESH_MESSAGE, REFRESH_ACTOR_ACTIVITY,REFRESH_UNDERGROUND_ACTIVITY, REFRESH_CONTACT_INFO]

REFRESH_TOPIC >> TOPIC_UNLOAD_S3 >> TOPIC_OPENSEARCH_TRANSFER >> MOVE_TOPIC_FILES >> DELETE_TOPIC_FILES >> POST_BATCH_STEP
REFRESH_CHANNEL >> CHANNEL_UNLOAD_S3 >> CHANNEL_OPENSEARCH_TRANSFER >> MOVE_CHANNEL_FILES >> DELETE_CHANNEL_FILES >> POST_BATCH_STEP
REFRESH_MESSAGE >> MESSAGE_UNLOAD_S3 >> MESSAGE_OPENSEARCH_TRANSFER >> MOVE_MESSAGE_FILES >> DELETE_MESSAGE_FILES >> POST_BATCH_STEP
REFRESH_ACTOR_ACTIVITY >> ACTOR_ACTIVITY_UNLOAD_S3 >> ACTOR_ACTIVITY_OPENSEARCH_TRANSFER >> MOVE_ACTOR_ACTIVITY_FILES >> DELETE_ACTOR_ACTIVITY_FILES >> POST_BATCH_STEP
REFRESH_UNDERGROUND_ACTIVITY >> UNDERGROUND_ACTIVITY_UNLOAD_S3 >> UNDERGROUND_ACTIVITY_OPENSEARCH_TRANSFER >> MOVE_UNDERGROUND_ACTIVITY_FILES >> DELETE_UNDERGROUND_ACTIVITY_FILES >> POST_BATCH_STEP
REFRESH_CONTACT_INFO >> CONTACT_INFO_UNLOAD_S3 >> CONTACT_INFO_OPENSEARCH_TRANSFER >> MOVE_CONTACT_INFO_FILES >> DELETE_CONTACT_INFO_FILES >> POST_BATCH_STEP
############################################################################
############################################################################
