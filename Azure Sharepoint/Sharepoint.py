"""
############################################################################################################################
#===========================================================================================================================
#EQUINIX CORPORATION - All Rights Reserved.
#---------------------------------------------------------------------------------------------------------------------------
#
#Script Name  : sharepoint_api.py
#Purpose      : This script will connect to microsoft azure share point site and download the buisiness files
#               and uploaded the data to exasol tables of corresponding process.
#Version      : 0.1
#---------------------------------------------------------------------------------------------------------------------------
#Date            Updated By            Comments
#---------------------------------------------------------------------------------------------------------------------------
#14-June-2021    Anil kumar         inital version.

############################################################################################################################
"""
# --*-- encoding: utf-8 --*--
import sys
# import atexit
import os
import urllib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import base64
import json
import datetime
#import oracle_jobs as oj
# import logging
import pandas as pd
import yaml
import pyexasol as exa
import requests
import msal
import cx_Oracle
#import db_config

#Functions are starting hear

def sharepoint_download(move_to_archive):
     #Accessing Sharepoint using MSAL Username authentication
    print('-----------sharepoint_download started-------------')
    try:

        # Access scopes provided for the azure portal for application sharepoint_file_download
        # URL : https://aad.portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/Overview/appId/730e3e72-d964-46db-b2ba-fd59d73c6054/isMSAApp/
        SCOPES = [
            'Files.ReadWrite.All',
            'Sites.ReadWrite.All',
            'User.Read',
            'User.ReadBasic.All'
        ]
        app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
        accounts = app.get_accounts(username=SERVICE_ACCOUNT)
        result = None

        if len(accounts) > 0:
            result = app.acquire_token_silent(SCOPES, account=accounts[0])

        if result is None:
            result = app.acquire_token_by_username_password(
                SERVICE_ACCOUNT, SERVICE_ACCOUNT_KEY, scopes=SCOPES)
            #print(result)

        if 'access_token' in result:

            # Query the graph to get the Site ID information using Site Name
            result_site_id = requests.get(f'{ENDPOINT}/sites/{SHAREPOINT_HOSTNAME}:/sites/{SITE_NAME}',
                                          headers={'Authorization': 'Bearer ' + result['access_token']})
            site_info = result_site_id.json()
            #print("site info is result_site_id.json()::::", site_info)
            site_id = site_info['id']
            print("site_id is site_info['id']:: :", site_id)

            # Query the graph to get the Drive ID information using Site ID
            result_drive_id = requests.get(f'{ENDPOINT}/sites/{site_id}/drive',
                                           headers={'Authorization': 'Bearer ' + result['access_token']})
            drive_info = result_drive_id.json()
            #print("drive_info is result_drive_id.json() :::",drive_info)
            result_drive_id.raise_for_status()
            print("result_drive_id.raise_for_status() is ::: ",result_drive_id.raise_for_status())
            drive_id = drive_info['id']
            print("drive_id is drive_info['id']  ::::", drive_id)

            # Query the graph to get the Folder ID information using Drive ID
            item_path = item_path_folder
            item_url = urllib.parse.quote(item_path)
            result_folder_id = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{item_url}',
                                            headers={'Authorization': 'Bearer ' + result['access_token']})
            item_info = result_folder_id.json()
            #print("item_info is result_folder_id.json()::::", item_info)
            folder_id = item_info['id']
            #print("folder_id item_info['id']::::", folder_id)

            # Query the graph to get the File Name information using Drive ID and Folder ID
            result_file_name = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{folder_id}/children',
                                            headers={'Authorization': 'Bearer ' + result['access_token']})
            children = result_file_name.json()['value']
            #print("result_file_name.json():::",result_file_name.json())
            #print("children values is result_file_name.json()['value']:::",children)
            for item in children:
                print(item['name'])
				# Query the graph to get the File ID
                print("Getting file information from Sharepoint . . . . . ")
                now = datetime.datetime.now()
                cmy=str(now.month)+'_'+str(now.year)+'/'
                file_url = urllib.parse.quote(FILE_PATH +cmy+ item['name'])
                #file_url = urllib.parse.quote(FILE_PATH + item['name'])
                print('file_url  ',file_url)
                file_nn, file_ee    = os.path.splitext('{}'.format(item['name']))
                print('file_nn :', file_nn)
                print('file_ee :', file_ee)
                result_access_token = result['access_token']
                result_file_id = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{file_url}',
                                            headers={'Authorization': 'Bearer ' + result['access_token']})
                file_info = result_file_id.json()
                print('file_info :',file_info)
                file_id = file_info['id']
                print('file_id :',file_id)
                file_name = file_info['name']
                print('file_name  ',file_name)
                if result_file_id.status_code == 200 and (file_ee == '.xls' or file_ee == '.xlsx' or file_ee == '.csv'):
                    print("File Available for Download . . . . . ")
                    print("Downloading . . . . . ")
                    result_download = requests.get(f'{ENDPOINT}/drives/{drive_id}/items/{file_id}/content',
                                                headers={'Authorization': 'Bearer ' + result['access_token']})
                    open('{}/{}'.format(stage_path,file_name), 'wb').write(result_download.content)
                    #print("result_download.content::",result_download.content)
                    #print('result_download.json() file::',result_download.json())
                    check_file_extension(file_ee, file_nn)
                    archival_process(ENDPOINT,drive_id, file_nn, file_id, result_access_token, file_ee)
                elif file_ee == '.eml':
                    print("Discarding the .eml file")
                    archival_process(ENDPOINT,drive_id, file_nn, file_id, result_access_token, file_ee)
                    print(result_file_id.status_code)
                else:
                    print("No xlxs or csv file available in sharepoint . . . . . exiting the application!")
                    print(result_file_id.status_code)
                    exit(0);
            #print("No file's available in sharepoint folder. . . . . check with business if you are expecting one!")
        else:
            raise Exception('no access token in result')

    except Exception as msg:
        log_message = ' Failed - While connecting to Sharepoint '
        log_level = 'ERROR'
        exception_handler(log_message, msg, log_level)


def check_file_extension(file_ee, file_nn):
    # Check wheather its a CSV file or XLSX file and process accordingly
    try:
        # file_n, file_e = os.path.splitext('{}'.format(fname_with_extn))
        print(file_nn)
        print(file_ee)
        process_file_name = file_nn + file_ee
        print('process_file_name (file_nn + file_ee) is  :', process_file_name)
        if file_ee == '.xls' or file_ee == '.xlsx':
            print("Reading the Excel file . . . . . ")
            process_excel(process_file_name, file_nn)
            exasol_stg(process_file_name, file_nn, file_ee)
        elif file_ee == '.csv':
            print("Reading the CSV file . . . . . ")
            exasol_stg(process_file_name, file_nn, file_ee)
        else:
            print('This API can process only CSV or Excel files')
            exit(0);
    except Exception as msg:
        log_message = ' Failed - While checking file extension '
        log_level = 'ERROR'
        exception_handler(log_message, msg, log_level)

def process_excel(process_file_name, file_nn):
    #Convert Excel files to CSV format UTF - 8 Compatible
    try:
        print("Converting Excel file to CSV format . . . . . ")
        file = open('{}/{}'.format(stage_path,process_file_name))

        if "SY" in process_file_name:
            print('Sydney File')
            excel_dataframe = pd.read_excel('{}/{}'.format(stage_path,process_file_name),engine='openpyxl',parse_dates=p_parse_dates_sy)
        else:
            print('Non-Sydney File')
            excel_dataframe = pd.read_excel('{}/{}'.format(stage_path,process_file_name),parse_dates=p_parse_dates_sy,skiprows=15)
        """
		lst = []
        for sh, data in excel_dataframe.items():
            lst.append(sh)
		"""
        values = ['Employee Name','ID','Pay Code(s)','Date','Schedule','Scheduled Total','Worked Entity','Worked Location',
                      'Worked Cost Center','Worked Project','Worked Supervisor','Hours Worked','Hours Worked Outside of Schedule','On Call $',
                      'Pager Pay','Regular','Overtime','Double Time','Double Time Night','Double Time Swing',
                      'In Punch.1','In Punch Comments: Notes.1','Out Punch.1','Out Punch Comments: Notes.1',
                      'In Punch.2','In Punch Comments: Notes.2','Out Punch.2','Out Punch Comments: Notes.2',
                      'In Punch.3','In Punch Comments: Notes.3','Out Punch.3','Out Punch Comments: Notes.3',
                      'In Punch.4','In Punch Comments: Notes.4','Out Punch.4','Out Punch Comments: Notes.4',
                      'In Punch.5','In Punch Comments: Notes.5','Out Punch.5','Out Punch Comments: Notes.5']
        df = pd.DataFrame(excel_dataframe, columns=values).fillna(value='')
        df['File name']=process_file_name
        df['Stg Load Dt']=datetime.datetime.now()
        df1 = df.rename(columns = str.upper)
        df1.to_csv('{}/{}.csv'.format(stage_path,file_nn),index=False, header=True,encoding='utf-8')
        # print(excel_dataframe)
        # exasol_stg()

    except Exception as msg:
        log_message = ' Failed - While Converting Excel files to CSV format UTF - 8 Compatible '
        log_level = 'ERROR'
        print(log_message, msg)
        exception_handler(log_message, msg, log_level)

def exasol_stg(process_file_name, file_nn, file_ee):
    # The file is read into pandas dataframe and loaded to Exassol stage table
    try:
        print("...Processing the CSV file . . . . . ")
        file = open('{}/{}.csv'.format(stage_path, file_nn),'r',encoding='utf-8')
        rec_cnt = len(file.readlines())
        print("Record Count of the file downloaded from sharepoint len(file.readlines()):: ", rec_cnt)
        if rec_cnt > 1:
            conn = exa_connector(exa_dsn_name, exa_user_name, exa_user_pass)
            exa_query = truncate_query.format(exa_stg_tbl)
            conn.execute(exa_query)
            result = conn.export_to_pandas(export_query.format(exa_stg_tbl))
            # p_col_nm = list(result['COLUMN_NAME'].str.lower())
            # print(result)
            p_col_nm = list(result['COLUMN_NAME'])
            print("p_col_nm list(result['COLUMN_NAME']) ::: ", p_col_nm)
            print("process_name is inside the exasol_stg function :::",process_name)
            if p_col_nm :
                if process_name in ['EMP', 'EMP_ADP']:
                    import_params = {'columns': p_col_nm}
                    with open('{}/{}.csv'.format(stage_path, file_nn), 'r', encoding='utf-8') as file:
                        for csv_dataframe in pd.read_csv(file, dtype='unicode', encoding='utf-8', chunksize=10000,iterator=True, keep_default_na=False):
                            conn.import_from_pandas(csv_dataframe, ('EXA_STG', '{}'.format(exa_stg_tbl)),import_params=import_params)
                else:
                    import_params = {'columns': p_col_nm}
                    with open('{}/{}.csv'.format(stage_path, file_nn), 'r', encoding='utf-8') as file:
                        for csv_dataframe in pd.read_csv(file, dtype='unicode', encoding='utf-8', chunksize=10000,iterator=True,parse_dates=p_parse_dates, keep_default_na=False):
                            conn.import_from_pandas(csv_dataframe, ('EXA_STG', '{}'.format(exa_stg_tbl)),import_params=import_params)
                        print(" <<<<< Successfully Loaded to Exasol Stage Table >>>>> ")
                        # os.remove(os.path.join('{}/{}.csv'.format(stage_path, file_nn)))
            else:
                print("<<< Please check the stage table in Config file >>> ")
                exit(0);
            conn.close()
            exasol_tgt(file_nn)
            file.close()
            for filename in os.listdir(stage_path):
                full_path = os.path.join(stage_path, filename)
                if os.path.isfile(full_path):
                    os.remove(full_path)
                    print("<<< Stage file of " + filename + " has been deleted from staging layer >>>")

        else:
            print('Empty File')
            exit(0);

    except Exception as msg:
        log_message = ' Failed - While loading the file to stage table: '
        log_level = 'ERROR'
        print(log_message,msg)
        exception_handler(log_message, msg, log_level)

def exa_connector(exa_dsn_name, exa_user_name, exa_user_pass):
    # Connector for exasol
    try:
        print("Connecting to Exasol . . . . . ")
        conn = exa.connect(dsn=exa_dsn_name, user=exa_user_name, password=exa_user_pass, socket_timeout=6000,encryption=True)
        print(" <<<<< Successfully Connected to Exasol >>>>> ")
        return conn
    except Exception as msg:
        log_message = ' Failed - While connecting to Exasol '
        log_level = 'ERROR'
        exception_handler(log_message, msg, log_level)


def exasol_tgt(file_nn):
    # Merge from exasol stage table to final table
    try:
        print("Merging to exassol target table")
        conn = exa_connector(exa_dsn_name, exa_user_name, exa_user_pass)
        exa_query = (merge_query).format(exa_dm_tbl, exa_stg_tbl)
        conn.execute(exa_query)
        print(" <<<<< Successfully Loaded to Exasol DM Final Table >>>>> ")
        conn.close()
    except Exception as msg:
        log_message = ' Failed - While loading the final table from stage table: '
        log_level = 'ERROR'
        exception_handler(log_message, msg, log_level)


def archival_process(ENDPOINT, drive_id, file_nn, file_id, result_access_token, file_ee):
    # Moving files to archive folder
    try:

        print("Moving file to Archive . . . . . ")
        archive_url = urllib.parse.quote(ARCHIVE_FILE_PATH)
        print('archive_url :', archive_url)
        result_archive_id = requests.get(f'{ENDPOINT}/drives/{drive_id}/root:/{archive_url}',
                                         headers={'Authorization': 'Bearer ' + result_access_token})
        archive_info = result_archive_id.json()
        print('archive_info :', archive_info)
        archive_folder_id = archive_info['id']
        print('archive_info :', archive_info['name'])
        new_file_name = file_nn + '{:%Y-%m-%d_%H%M%S}'.format(datetime.datetime.now()) + file_ee
        move_file = {'parentReference': {'id': archive_folder_id}, 'name': new_file_name}
        print('move_file  ', move_file)
        result_upload = requests.patch(f'{ENDPOINT}/drives/{drive_id}/items/{file_id}',
                                       headers={'Authorization': 'Bearer ' + result_access_token,
                                                'Content-Type': 'application/json'},
                                       data=json.dumps(move_file))

    except Exception as msg:
        log_message = ' Failed - While moving files to archive '
        log_level = 'ERROR'
        exception_handler(log_message, msg, log_level)


def exception_handler(log_message, msg, log_level):
    # Log exception details to Oracle DB
    try:
        print(msg)
        print(log_message)

        LUA_JOB_DETAIL_LOG(log_level, workflow_nm, 0, log_message, etl_proc_wid)
        if log_level == 'ERROR':
            raise msg

    except Exception as msg:
        print("Error")
        print(msg)
        raise

def LUA_JOB_DETAIL_LOG(log_level,task_name,num_rows,log_message,etl_proc_wid,exa_session_id = 0):
    try:
        print(num_rows)
        print(etl_proc_wid)
        print(log_message)
        data=(etl_proc_wid,log_level,log_message,num_rows,task_name,0,0,0,exa_session_id)
        connection = cx_Oracle.connect(ora_user_name, ora_user_pass, ora_dsn_name+ ":pooled")
        #print("Connected To Oracle Database version:", connection .version)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO CTL_LOGGING.LUA_JOB_DETAIL_LOG(ETL_PROC_WID,LOG_LEVEL,LOG_MESSAGE,ROWCOUNT,LUA_JOB_NAME,ROW_INSERTED, ROW_UPDATED, ROW_DELETED,EXA_SESSION_ID,LOG_TIME ) VALUES(    :v0,:v1,:v2,:v3,:v4,:v5,:v6,:v7,:v8,SYSDATE)",data)
        cursor.execute("COMMIT")
        cursor.close()
        connection.close()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("FAILED with error:", error.message)
        raise Exception(error.message)
        exit(1)

if __name__ == "__main__":
    if (len(sys.argv) == 5):
        print("Require arguments are passed in sharepoint_api_wrapper....!")
    else:
        print("Required aruguments are not passed in Sharepoint_api_wrapper, Please pass arguments and re-run the script.")
        sys.exit(1)
    try:
        # Anil:07-06-2021 file changes for consolidations.
        # input_config = sys.argv[1]
        input_env = sys.argv[1]
        workflow_nm = sys.argv[2]
        etl_proc_wid = sys.argv[3]
        process_name = sys.argv[4]  # passing the process name like : PBI_USAG,KPI,EMP and EMP_ADP
        log_message = (process_name+' process file - Started')
        msg = (process_name+'extract')
        log_level = 'INFO'
        #exception_handler(log_message, msg, log_level)
        # user_config_yaml = open(input_config)
        # user_config = yaml.load(user_config_yaml, Loader=yaml.FullLoader)
        global_config_yaml = open("C:\\Power BI Automation Scripts\\Scripts\\Config\\{}\\config.yaml".format(input_env))
		#global_config_yaml = open("C:\\Power BI Automation Scripts\\Scripts\\Config\\uat\\config.yaml")
        global_config = yaml.load(global_config_yaml, Loader=yaml.FullLoader)

        # Global variables starts here
        TENANT_ID_ENC = global_config["G_SHAREPOINT_CONFIG"]["TENANT_ID"]
        TENANT_ID = base64.b64decode(TENANT_ID_ENC).decode('ascii')
        AUTHORITY = global_config["G_SHAREPOINT_CONFIG"]["AUTHORITY"] + TENANT_ID
        CLIENT_ID_ENC = global_config["G_SHAREPOINT_CONFIG"]["CLIENT_ID"]
        CLIENT_ID = base64.b64decode(CLIENT_ID_ENC).decode('ascii')
        SERVICE_ACCOUNT = global_config["G_SHAREPOINT_CONFIG"]["SERVICE_ACCOUNT"]
        SERVICE_ACC_KEY_ENC = global_config["G_SHAREPOINT_CONFIG"]["SERVICE_ACCOUNT_KEY"]
        SERVICE_ACCOUNT_KEY = base64.b64decode(SERVICE_ACC_KEY_ENC).decode('ascii')
        SHAREPOINT_HOSTNAME = global_config["G_SHAREPOINT_CONFIG"]["SHAREPOINT_HOSTNAME"]
        ENDPOINT = global_config["G_SHAREPOINT_CONFIG"]["ENDPOINT"]
        TEMP_DIR = global_config["G_SHAREPOINT_CONFIG"]["TEMP_DIR"]

        exa_dsn_name = global_config["G_EXASOL_CONFIG"]["EXA_DSN_NAME"]
        exa_user_name = global_config["G_EXASOL_CONFIG"]["EXA_USER_NAME"]
        exa_user_pass_enc = global_config["G_EXASOL_CONFIG"]["EXA_USER_PASS"]
        exa_user_pass = base64.b64decode(exa_user_pass_enc).decode('ascii')

        ora_dsn_name = global_config["G_ORACLE_CONFIG"]["ORA_DSN_NAME"]
        ora_user_name = global_config["G_ORACLE_CONFIG"]["ORA_USER_NAME"]
        ora_user_pass_enc = global_config["G_ORACLE_CONFIG"]["ORA_USER_PASS"]
        ora_user_pass = base64.b64decode(ora_user_pass_enc).decode('ascii')

        p_dtype = global_config["G_PANDAS_CONFIG"]["P_DTYPE"]
        p_parse_dates = global_config["G_PANDAS_CONFIG"]["P_PARSE_DATES"]
        p_col_nm = global_config["G_PANDAS_CONFIG"]["P_COL_NM"]
        p_parse_dates_sy = global_config["G_PANDAS_CONFIG"]["P_PARSE_DATES_SY"]

        truncate_query = global_config["G_QUERY_CONFIG"]["TRUNCATE_QUERY"]
        export_query = global_config["G_QUERY_CONFIG"]["EXPORT_QUERY"]

        # Local Variables Starts here.
        #process_list=['PBI_USAGE','KPI','EMP','EMP_ADP']
        process_list = global_config["G_SHAREPOINT_CONFIG"]["PROCESS_LIST"].split(',')
        print("process_list is :",process_list)
        if process_name in process_list:
            SITE_NAME           = global_config["SHAREPOINT_CONFIG_"+process_name]["SITE_NAME"]
            FILE_PATH           = global_config["SHAREPOINT_CONFIG_"+process_name]["FILE_PATH"]
            ARCHIVE_FILE_PATH   = global_config["SHAREPOINT_CONFIG_"+process_name]["ARCHIVE_FILE_PATH"]
            stage_path          = global_config["PANDAS_CONFIG_"+process_name]["STAGE_PATH"]
            excel_column_count  = global_config["FILENAME_CONFIG_"+process_name]["EXCEL_COLUMN_COUNT"]
            exa_stg_tbl         = global_config["EXASOL_CONFIG_"+process_name]["EXA_STG_TBL"]
            exa_dm_tbl          = global_config["EXASOL_CONFIG_"+process_name]["EXA_DM_TBL"]
            merge_query         = global_config["QUERY_CONFIG_"+process_name]["MERGE_QUERY"]
        else:
            print("Pleas pass the process name as 3rd arugument")

        now1 = datetime.datetime.now()
        cmy1=str(now1.month)+'_'+str(now1.year)+'/'
        item_path_folder    = str(FILE_PATH)+ cmy1
        print('item_path_folder :', item_path_folder)
        move_to_archive = False
        sharepoint_download(move_to_archive)
        log_message = (process_name+' process file - Ended')
        print(log_message)
        log_level = 'INFO'
        exception_handler(log_message, msg, log_level)

    except Exception as msg:
        print("Error")
        print(msg)
        raise
