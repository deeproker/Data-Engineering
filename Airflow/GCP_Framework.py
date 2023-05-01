"""pylint code rating 7.95/10 """
from datetime import datetime, timedelta
import sys
import os
import os.path
import yaml
import requests
import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.hooks.base_hook import BaseHook
from google.cloud import bigquery
from airflow.models import Variable
import gcsfs

conn = BaseHook.get_connection('eqms')
eqms_id = conn.login
eqms_key = conn.password
#bucketName = 'analytics-eqms-dev'

FILE_NAME = os.path.join(os.path.dirname(__file__), 'configs/eqms_config.yaml')
#FILE_NAME = os.path.join(os.path.dirname(__file__), 'configs/dag_issue.yaml')
print(FILE_NAME)
CONFIG_YAML = open(FILE_NAME)
CONFIG = yaml.load(CONFIG_YAML, Loader=yaml.FullLoader)
ENV = Variable.get("ENV")
#ENV = 'dev'

poke_interval = int(CONFIG['sensor']['poke_interval'])
timeout = int(CONFIG['sensor']['timeout'])
project_id = CONFIG[ENV]['config']['project_id']
bucketName = CONFIG[ENV]['config']['bucketName']

keypair={"x-eqms-api-id": eqms_id,"x-eqms-api-key": eqms_key,'Content-Type': 'application/json'}

def recursive_bridge_data_fetch(rec_path,id,columns):
    """pulling bridge data"""
    try:
        appended_df = pd.DataFrame(columns=columns)
        df = pd.DataFrame(columns=columns)
        org_data = requests.get('https://equinix.myeqms.com'+rec_path,headers=keypair)
        org_data=org_data.json()
        org_data_keys = list(org_data['result'].keys())
        org_dataset_name = org_data_keys[0]
        #print(org_dataset_name)
        #print(os.path.basename(rec_path))
        #exit(0)
        if org_dataset_name == 'error_Code' and os.path.basename(rec_path) !='workflowactions':
            appended_df[columns[0]] = [0]
            appended_df[columns[1]] = [id]
            return appended_df
        if org_dataset_name == 'error_Code' and os.path.basename(rec_path) =='workflowactions':
            appended_df['auditId'] = [id]
            return appended_df
        if os.path.basename(rec_path) !='workflowactions':
            org_data = org_data['result']
            org_data_key = list(org_data.keys())
            org_data_key = org_data_key[0]
            org_data = org_data[org_data_key]
            count = len(org_data)
            #print(org_data)
            get_first_key=pd.DataFrame(org_data)
            #print(dd_ff.iloc[: , :1])
            get_first_key=list(get_first_key.iloc[: , :1])
            get_first_key=get_first_key[0]
            #print(get_first_key)
            #exit(0)
            org_data_list = []
            audit_id_list = []
            i = 0
            while i < count:
                org_data_id = org_data[i][get_first_key]#['organisationalAreaId']
                org_data_list.append(org_data_id)
                audit_id_list.append(id)
                # print(org_data_list)
                i = i + 1
            df[columns[0]] = org_data_list
            df[columns[1]] = audit_id_list
            appended_df = pd.concat([appended_df, df], ignore_index=True)
            return appended_df
            #print(appended_df)
        else:
            org_data = dict(org_data)
            org_data = org_data['result']['associatedFindings']
            i = 0
            while i < len(org_data):
                #print(org_data[i]['findingID'])
                #print(org_data[i]['associatedActions'])
                df = pd.DataFrame(org_data[i]['associatedActions'])
                df['findingID'] = org_data[i]['findingID']
                df['auditId'] = id
                appended_df = pd.concat([appended_df, df], ignore_index=True)
                i = i + 1
            # exit(0)
            #print(appended_df)
            return appended_df
    except Exception as msg:
        log_message = ' Failed - While recurcively fetching  '+rec_path
        log_level = 'ERROR'
        print(log_message, msg, log_level)
        sys.exit(1)
        raise

def recursive_issue_data_bridge_values(dataset_type,id,columns):
    """pulling bridge data"""
    try:
        appended_df = pd.DataFrame(columns=columns)
        get_data = requests.get('https://equinix.myeqms.com'+dataset_type,
                             headers=keypair)

        org_data=get_data.json()
        org_data = org_data['result']#['actions']
        print(list(org_data.keys())[0])
        #exit(0)
        if list(org_data.keys())[0]=='error_Code':
            appended_df['issueid']=[id]
            return appended_df
        else:
            appended_df = pd.DataFrame(columns=columns)
            if list(org_data.keys())[0] == 'values':
                df = pd.DataFrame(org_data['values'])
            elif list(org_data.keys())[0] == 'actions':
                df=pd.DataFrame(org_data['actions']) #metadataValues
            else:
                print('No Proper Bridge Dataset type entered . '
                      'Kindly verify the input field names as it is case sensitive')
                sys.exit(1)
            appended_df=pd.concat([appended_df,df], ignore_index=True)
            appended_df['issueid']=id
            print(appended_df['issueid'])
            return appended_df


    except Exception as msg:
        log_message = ' Failed - While extracting the Data '
        log_level = 'ERROR'
        print(log_message, msg, log_level)
        sys.exit(1)
        raise


def get_audit_issue_data_bridge(dataset_type,bridge_dataset_type):
    """pulling bridge data"""
    try:

        if dataset_type=='issues':
            get_data = requests.get('https://equinix.myeqms.com/api/issue/'+dataset_type,
                        headers=keypair)
        else :
            get_data = requests.get('https://equinix.myeqms.com/api/Audit/' + dataset_type,
                                headers=keypair)

        org_data=get_data.json()
        org_data=org_data['result']

        org_data_key = list(org_data.keys())
        org_data_key = org_data_key[0]
        org_data = org_data[org_data_key]
        #print(org_data[0]['auditId'])
        #exit(0)
        #print(org_data[0][bridge_dataset_type])
        if (bridge_dataset_type=='organisationalAreas' and dataset_type=='audits'):
            columns=['orgid','auditid']
            dataset_id = 'auditId'
        elif (bridge_dataset_type == 'organisationalAreas' and dataset_type == 'findings'):
            columns = ['orgid', 'findingid']
            dataset_id = 'findingId'
        elif (bridge_dataset_type == 'organisationalAreas' and dataset_type == 'issues'):
            columns = ['orgid', 'issueid']
            dataset_id = 'issueId'
        elif (bridge_dataset_type == 'workflowActions' and dataset_type == 'issues'):
            columns = ['actionTypeId', 'sequence', 'title', 'instructions',
                       'actioneeType', 'actioneeUserId', 'actioneeUserFirstName',
                       'actioneeUserSurname', 'actioneeGroupId', 'actioneeGroupCode',
                       'actioneeGroupTitle', 'issuedDate', 'targetDate', 'completedDate',
                       'status', 'result','issueid']
            dataset_id = 'issueId'
        elif (bridge_dataset_type == 'metadataValues' and dataset_type == 'issues'):
            columns = ['metadataTypeId', 'title', 'dataType', 'fieldType',
                       'displayLabel', 'tooltip', 'value','issueid']
            dataset_id = 'issueId'
        elif (bridge_dataset_type == 'workflowActions' and dataset_type == 'audits'):
            columns = ['actionTypeId', 'sequence', 'title', 'instructions',
                       'actioneeType', 'actioneeUserId', 'actioneeUserFirstName',
                       'actioneeUserSurname', 'actioneeGroupId', 'actioneeGroupCode',
                       'actioneeGroupTitle', 'issuedDate', 'targetDate', 'completedDate',
                       'status', 'result','findingID','auditId']
            dataset_id = 'auditId'
            #print(columns)
            #exit(0)
        else:
            print('No Proper Bridge Dataset type entered . '
                  'Kindly verify the input field names as it is case sensitive')
            sys.exit(1)

        final_df = pd.DataFrame(columns=columns)
        #print(final_df)
        #exit(0)
        #nested_columns_list=[bridge_dataset_type]
        i=0
        while i<len(org_data):
            if dataset_type=='issues' and bridge_dataset_type != 'organisationalAreas':
                df=recursive_issue_data_bridge_values(org_data[i][bridge_dataset_type],
                                                      org_data[i][dataset_id],columns)
            elif dataset_type=='audits' and bridge_dataset_type == 'workflowActions':
                df = recursive_bridge_data_fetch('/api/audit/audits/'+
                str(org_data[i][dataset_id])+'/findings/workflowactions',
                                                 org_data[i][dataset_id], columns)
            else:
                df=recursive_bridge_data_fetch(org_data[i][bridge_dataset_type],
                                               org_data[i][dataset_id],columns)
            final_df=pd.concat([final_df, df], ignore_index=True)
            #print(df)
            i=i+1
        file_save = gcsfs.GCSFileSystem()
        #bucketName = 'analytics-devops-test1'
        #dataset_file_type = os.path.basename(dataset_type)
        dataset_file_type = dataset_type + '_' + bridge_dataset_type
        filename = bucketName + '/' + dataset_file_type + '.csv'
        final_df=final_df.dropna(how='all')
        with file_save.open(filename, 'w',encoding="utf-8", newline='') as file_open:
            final_df.to_csv(file_open, index=False)
    except Exception as msg:
        log_message = ' Failed - While extracting the Data '
        log_level = 'ERROR'
        print(log_message, msg, log_level)
        sys.exit(1)
        raise

def get_data(dataset_type):
    """ get data """
    try:
        get_data = requests.get('https://equinix.myeqms.com/api' + dataset_type,
                                headers=keypair)

        org_data = get_data.json()
        org_data = org_data['result']
        org_data_key = list(org_data.keys())
        org_data_key = org_data_key[0]
        org_data = org_data[org_data_key]
        # print(org_data)
        df = pd.json_normalize(org_data)
        #print(df)
        file_save = gcsfs.GCSFileSystem()
        dataset_file_type = os.path.basename(dataset_type)
        filename = bucketName + '/' + dataset_file_type + '.csv'
        with file_save.open(filename, 'w', encoding="utf-8", newline='') as file_open:
            df.to_csv(file_open, index=False)

        # df.to_csv('Audit_Blank_Test.csv', index=False,encoding="utf-8")
    except Exception as msg:
        log_message = ' Failed - While extracting the Data '
        log_level = 'ERROR'
        print(log_message, msg, log_level)
        sys.exit(1)
        raise


def task_failure(context):
    """failure task setup"""
    query_str = """CALL `{v_project_id}.BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING`\
                    ('{V_Batch_Name}','{V_Task_Name}',null,null,'{V_Task_Name}','FAILED'\
                    ,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),null,null,null);"""
    query_str1 = query_str.format(V_Batch_Name=context['dag'].dag_id,
                                  V_Task_Name=context['task_instance'].task_id,
                                  v_project_id=project_id)
    print(query_str1)
    bq_client = bigquery.Client(project=project_id)
    bq_client.query(query_str1).result()


def task_success(context):
    """task success setup"""
    query_str = """CALL `{v_project_id}.BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING`\
                    ('{V_Batch_Name}','{V_Task_Name}',null,null,'{V_Task_Name}','COMPLETED'\
                    ,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),null,null,null);"""
    query_str1 = query_str.format(V_Batch_Name=context['dag'].dag_id,
                                  V_Task_Name=context['task_instance'].task_id,
                                  v_project_id=project_id)
    print(query_str1)
    bq_client = bigquery.Client(project=project_id)
    bq_client.query(query_str1).result()


def pre_batch(**kwargs):
    """pre batch setup"""
    context = kwargs
    print(context)
    query_str = """CALL`{v_project_id}.BQ_CTL_METADATA.BQ_PRE_BATCH_CTL_LOGGING`
    ('{V_Batch_Name}');"""
    query_str1 = query_str.format(V_Batch_Name=kwargs['dag'].dag_id, v_project_id=project_id)
    print(query_str1)
    bq_client = bigquery.Client(project=project_id)
    bq_client.query(query_str1).result()


def post_batch(**kwargs):
    """post batch setup"""
    context = kwargs
    print(context)
    query_str = """CALL `{v_project_id}.BQ_CTL_METADATA.BQ_POST_BATCH_CTL_LOGGING`
    ('{V_Batch_Name}');"""
    query_str1 = query_str.format(V_Batch_Name=kwargs['dag'].dag_id, v_project_id=project_id)
    print(query_str1)
    bq_client = bigquery.Client(project=project_id)
    bq_client.query(query_str1).result()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 27),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}


dag = DAG(
    dag_id='EQMS_AUDIT',
    default_args=default_args,
    schedule_interval=None,
    tags=['EQMS'],
    catchup=False,
    max_active_runs=1
)

PRE_BATCH_STEP = PythonOperator(
    task_id="PRE_BATCH_STEP",
    python_callable=pre_batch,
    provide_context=True,
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,dag=dag
)
eqms_extract_gcs_audit_findings = PythonOperator(task_id='eqms_extract_gcs_audit_findings',\
                                  python_callable=get_data,op_args=['/audit/findings'],\
                                  on_failure_callback=task_failure,\
                                  on_success_callback=task_success,dag=dag)

eqms_extract_gcs_audit_audittypes = PythonOperator(task_id='eqms_extract_gcs_audit_audittypes',
                                    python_callable=get_data,op_args=['/audit/audittypes'],
                                    on_failure_callback=task_failure,
                                    on_success_callback=task_success,dag=dag)
eqms_extract_gcs_audit_findingtypes = PythonOperator(task_id='eqms_extract_gcs_audit_findingtypes',
                                      python_callable=get_data,op_args=['/audit/findingtypes'],
                                      on_failure_callback=task_failure,
                                      on_success_callback=task_success,dag=dag)
eqms_extract_gcs_audit = PythonOperator(task_id='eqms_extract_gcs_audit',
                                        python_callable=get_data,
                                        op_args=['/audit/audits'],
                                        on_failure_callback=task_failure,
                                        on_success_callback=task_success,dag=dag)
eqms_extract_gcs_audit_action = PythonOperator(task_id='eqms_extract_gcs_audit_action',
                                        python_callable=get_audit_issue_data_bridge,
                                        op_args=['audits','workflowActions'],
                                        retries=2,
                                        on_failure_callback=task_failure,
                                        on_success_callback=task_success,dag=dag)
eqms_extract_gcs_audit_org = PythonOperator(task_id='eqms_extract_gcs_audit_org',
                                                   python_callable=get_audit_issue_data_bridge,
                                                   op_args=['audits', 'organisationalAreas'],
                                                   retries=2,
                                                   on_failure_callback=task_failure,
                                                   on_success_callback=task_success
                                                       ,dag=dag)
"""
eqms_extract_findings_org_bridge = PythonOperator(task_id='eqms_extract_findings_org_bridge',\
                                                  python_callable=get_audit_issue_data_bridge,\
                                                  op_args=['findings','organisationalAreas'],\
                                                  on_failure_callback=task_failure,\
                                                  on_success_callback=task_success,dag=dag)"""
eqms_load_bq_stg_audit = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit',
    bucket=bucketName,
    source_objects=['audits.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,
    dag=dag
)

eqms_load_bq_stg_audittype = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audittype',
    bucket=bucketName,
    source_objects=['audittypes.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDITTYPE',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,
    dag=dag
)

eqms_load_bq_stg_audit_findingtypes = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit_finding_types',
    bucket=bucketName,
    source_objects=['findingtypes.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT_FINDINGTYPES',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,
    dag=dag
)

eqms_load_bq_stg_audit_findings = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit_findings',
    bucket=bucketName,
    source_objects=['findings.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT_FINDINGS',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success
        ,dag=dag
)

eqms_load_bq_stg_audit_action = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit_action',
    bucket=bucketName,
    source_objects=['audits_workflowActions.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT_ACTION_BRIDGE',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success
        ,dag=dag
)
"""
eqms_load_bq_stg_audit_findings_org = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit_findings_org',
    bucket=bucketName,
    source_objects=['findings_organisationalAreas.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT_FINDINGS_ORG_BRIDGE',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    bigquery_conn_id='ibx_ops_dev',
    google_cloud_storage_conn_id='ibx_ops_dev',
    on_failure_callback=task_failure,
    on_success_callback=task_success
        ,dag=dag
)
"""
eqms_load_bq_stg_audit_org = GCSToBigQueryOperator(
    task_id='eqms_load_bq_stg_audit_org',
    bucket=bucketName,
    source_objects=['audits_organisationalAreas.csv'],
    schema_fields=None,
    schema_object=None,
    destination_project_dataset_table=project_id+'.EQMS_QUALSYS.STG_EQMS_AUDIT_ORG_BRIDGE',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    allow_quoted_newlines=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success
        ,dag=dag
)

eqms_load_bq_tgt_audit = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT`"
        "(@v_batch_name,@v_task_name,@v_ctl_project)".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id }}
                  ],
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,
    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,dag=dag)


eqms_load_bq_tgt_audittype = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audittype',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_AUDITTYPE`"
        "(@v_batch_name,@v_task_name,@v_ctl_project)".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id }}
                  ],
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,
    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,dag=dag)

eqms_load_bq_tgt_audit_findingtype = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit_findingtype',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_FINDINGTYPE`"
        "(@v_batch_name,@v_task_name,@v_ctl_project)".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id }}
                  ],
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,
    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,dag=dag)

eqms_load_bq_tgt_audit_findings = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit_findings',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_FINDINGS`"
        "(@v_batch_name,@v_task_name,@v_ctl_project)".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id }}
                  ],

    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,
    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,dag=dag)
    

eqms_load_bq_tgt_audit_org = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit_org',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_ORG_BRIDGE`"
        "(@v_batch_name,@v_task_name,@v_ctl_project);".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id}}
                  ],
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,
    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success
        ,dag=dag)

eqms_load_bq_tgt_audit_action = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit_action',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_ACTION_BRIDGE`"
        "(@v_batch_name,@v_task_name,@v_ctl_project);".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id}}
                  ],
     poke_interval=poke_interval,
    timeout=timeout,    mode='reschedule',
    retries=0,    provide_context=True,
    on_failure_callback=task_failure,
    on_success_callback=task_success,dag=dag)
"""
eqms_load_bq_tgt_audit_findings_org = BigQueryExecuteQueryOperator(
    task_id='eqms_load_bq_tgt_audit_findings_org',
    sql="call `{v_project_id}.EQMS_QUALSYS.SP_DM_EQMS_AUDIT_FINDINGS_ORG_BRIDGE`"
        "(@v_batch_name,@v_task_name,@v_ctl_project);".format(v_project_id=project_id),
    use_legacy_sql=False,
    query_params=[{'name': 'v_batch_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ dag["dag_id"] }}'}},
                  {'name': 'v_task_name', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': '{{ task_instance["task_id"] }}'}},
                  {'name': 'v_ctl_project', 'parameterType': {'type': 'STRING'},
                   'parameterValue': {'value': project_id}}
                  ],
     poke_interval=poke_interval,
    timeout=timeout,    mode='reschedule',
    retries=0,    provide_context=True,
    on_failure_callback=task_failure,    on_success_callback=task_success,dag=dag)"""
POST_BATCH_STEP = PythonOperator(
    task_id="POST_BATCH_STEP",
    python_callable=post_batch,
    provide_context=True,
    poke_interval=poke_interval,
    timeout=timeout,
    mode='reschedule',
    retries=0,dag=dag)

PRE_BATCH_STEP >> eqms_extract_gcs_audit
eqms_extract_gcs_audit >> eqms_extract_gcs_audit_audittypes
eqms_extract_gcs_audit_audittypes >> eqms_extract_gcs_audit_findingtypes
eqms_extract_gcs_audit_audittypes >>  eqms_load_bq_stg_audittype
eqms_extract_gcs_audit_findingtypes >> eqms_load_bq_stg_audit_findingtypes
eqms_load_bq_stg_audit_findingtypes >> eqms_load_bq_tgt_audit_findingtype
eqms_load_bq_stg_audittype >> eqms_load_bq_tgt_audittype
eqms_extract_gcs_audit >> eqms_load_bq_stg_audit
eqms_load_bq_stg_audit >> eqms_load_bq_tgt_audit
eqms_extract_gcs_audit_audittypes >> eqms_extract_gcs_audit_org
eqms_extract_gcs_audit_audittypes >> eqms_extract_gcs_audit_findings
eqms_extract_gcs_audit >> eqms_extract_gcs_audit_action
eqms_extract_gcs_audit_org >> eqms_load_bq_stg_audit_org
eqms_load_bq_stg_audit_org >> eqms_load_bq_tgt_audit_org
#eqms_extract_findings_org_bridge >> eqms_extract_gcs_audit_findings
eqms_extract_gcs_audit_findings >> eqms_load_bq_stg_audit_findings
eqms_load_bq_stg_audit_findings >> eqms_load_bq_tgt_audit_findings
eqms_extract_gcs_audit_action >> eqms_load_bq_stg_audit_action
eqms_load_bq_stg_audit_action >> eqms_load_bq_tgt_audit_action
#eqms_extract_gcs_audit_action >> eqms_extract_findings_org_bridge
#eqms_extract_findings_org_bridge >> eqms_load_bq_stg_audit_findings_org
#eqms_load_bq_stg_audit_findings_org >> eqms_load_bq_tgt_audit_findings_org
eqms_load_bq_stg_audit_findings >> eqms_load_bq_tgt_audit_findings
[eqms_load_bq_tgt_audit,
 eqms_load_bq_tgt_audit_findings,eqms_load_bq_tgt_audit_action,
 eqms_load_bq_tgt_audittype,eqms_load_bq_tgt_audit_findingtype,eqms_load_bq_tgt_audit_org
 ]>>POST_BATCH_STEP
