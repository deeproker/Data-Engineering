import requests
import msal
import time
import logging
import configparser as cp
import sys
import os
import json
sys.path.append(os.environ.get('INFA_SHARED')+"/Scripts/exasol/secure_config/")
import secure_config

config = cp.ConfigParser(interpolation=None)
aes_encrypted_filepath = "/home/informatica/.env/.env_config_onprem_encryption.ini"
kms_encrypted_filepath = "/home/informatica/.env/.env_config_cloud_encryption.ini"

try:
    if secure_config.is_gkms_down == 'Y':
        op = secure_config.aes256_decrypt_file_to_str(aes_encrypted_filepath)
    else:
        op = secure_config.decrypt_file_to_str(kms_encrypted_filepath)

    config.read_string(op)
    url_api = config.get('power-bi-config', 'pbi_url_api')
    authority_url = config.get('power-bi-config', 'pbi_authority_url')
    resource = config.get('power-bi-config', 'pbi_resource')
    pbi_username = config.get('power-bi-config', 'pbi_username')
    pbi_password = config.get('power-bi-config', 'pbi_password')
    client_id = config.get('power-bi-config', 'pbi_client_id')
    scope= config.get('power-bi-config', 'scope')
except Exception as e:
    print("Error: Failed to read the Configuration file ==> {e}".format(e=str(e)))
    sys.exit(1)

def get_msal_token():
    try:
        app = msal.PublicClientApplication(client_id, authority=authority_url)
        result = app.acquire_token_by_username_password(pbi_username, pbi_password,scopes=[scope])
        return result['access_token']
    except Exception as e:
        print("Error: Failed to get the Authorization Token for power bi client ==> {e}".format(e=str(e)))
        sys.exit(1)

class dataset:
    def __init__(self, Group_ID, DatasetID):
        self.Group_ID = Group_ID
        self.DatasetID = DatasetID
        #print(self.Group_ID+'   '+self.DatasetID)

    def refresh_dataset(self):
        try:
##############  Below Part will Autorize the Power Bi client  ##########
            access_token = get_msal_token()
            #print(access_token)

            url_refresh_request = url_api + 'groups/' + self.Group_ID + '/datasets/' + self.DatasetID + '/refreshes'

            post_refresh_request = requests.post(url_refresh_request,
                                         headers={'Authorization': 'Bearer ' + access_token})

            time.sleep(10)

            url_latest_status = url_refresh_request+'?$top=1'

            get_current_status = requests.get(url_latest_status,
                                      headers={'Authorization': 'Bearer ' + access_token})

            check_if_running = get_current_status.json()
            waiting_time=0
            while check_if_running['value'][0]['status'] == 'Unknown':
                time.sleep(2)
                waiting_time=waiting_time+2
                print('Current Waiting Time Elapsed : ' +str(waiting_time)+'Sec')
                if waiting_time == 3600:
                    print('Refresh taking more than 60 minutes , Kindly check .Cancelling the Request')
                    exit(1)
                else:
                    print('Current Status of Refresh for the dataset is : ' + check_if_running['value'][0]['status'] + ' Thus need to wait another 2 sec')
                    get_current_status = requests.get(url_latest_status,
                                                        headers={'Authorization': 'Bearer ' + access_token})

                    check_if_running = get_current_status.json()

            if check_if_running['value'][0]['status'] == 'Failed':
                print(
                    'Data Refresh Has Failed with below error description :                ' + str(check_if_running['value']))
                exit(1)
            else:
                print('Data refresh has been successfully completed on :' + check_if_running['value'][0]['endTime'])
        except Exception as msg:
            log_message = ' Failed - While trying to Refresh Dataset '
            log_level = 'ERROR'
            print(log_message, msg, log_level)
            exit(1)
            raise

class group_reports:
    def __init__(self, Group_ID,Report_ID):
        self.Group_ID = Group_ID
        self.Report_ID = Report_ID
        #print(self.Group_ID+'   '+self.DatasetID)

    def get_report_name(self):
        try:
##############  Below Part will Autorize the Power Bi client  ##########
            access_token = get_msal_token()

            self.url_group = url_api + 'groups/' + self.Group_ID + '/reports'

            get_report_names = requests.get(self.url_group,
                                             headers={'Authorization': 'Bearer ' + self.access_token})


            report_names = get_report_names.json()
            report_list_json=list(report_names['value'])
            #report_names = ['Activities' , 'Activity by IBX' , 'Activity by IBX - Summary']
            count_of_reports=len(report_list_json)
            print(json.dumps(report_list_json[1]["name"]))
            print(count_of_reports)
            #exit(0)
            i=0

            body_current = [{"name": "day", "value": "10"}]

            while i < count_of_reports :
                report_list_json_name=json.dumps(report_list_json[i]["name"])
                report_list_json_name=str.replace(report_list_json_name,'\"','')
                #report_list_json=report_names[i]
                body_prev = {"name": "report", "value": []}
                body_prev["value"]=report_list_json_name
                body_current.append(body_prev)
                i=i+1

            print(type(body_current))
            body_param = {"parameterValues": []}
            body_param["parameterValues"]=body_current
            print(type(body_param))
            body_tmp = {"format": "CSV", "paginatedReportConfiguration": []}
            body_tmp["paginatedReportConfiguration"]=body_param

            body=body_tmp
            #test_body={'format': 'CSV','paginatedReportConfiguration':{'parameterValues':[{'name': 'report', 'value': 'Activities'}, {'name': 'day', 'value': '3'}]}}

            print(type(body))
            print(body)

            self.post_refresh_request = requests.post(self.url_group+'/'+self.Report_ID+'/ExportTo',json.dumps(body),#data=open('sample.json', 'rb').read(),#,#open('sample.json', 'rb').read(),#,
                                        headers={'Authorization': 'Bearer ' + self.access_token, 'Content-Type' : 'application/json'})
            #print(self.post_refresh_request)
        except Exception as msg:
            log_message = ' Failed - While fetching report names '
            log_level = 'ERROR'
            print(log_message, msg, log_level)
            exit(1)
            raise

    def download_report(self):
        try:
            export=self.post_refresh_request.json()
            export_id=export['id']
            print('Export id generated is : '+export_id)
            export_status=export['status']
            waiting_time=0
            url_report = url_api + '/reports/' + self.Report_ID

            while export_status == 'NotStarted' or export_status == 'Running':
                time.sleep(2)
                waiting_time = waiting_time + 2
                print('Current Waiting Time Elapsed : ' + str(waiting_time) + 'Sec')
                export = requests.get(url_report + '/exports/' + export_id,
                                      headers={'Authorization': 'Bearer ' + self.access_token,'Content-Type': 'application/json'})
                export=export.json()
                export_status = export['status']
                if waiting_time == 3600:
                    print('Download is taking more than 60 minutes , Kindly check .Cancelling the Request')
                    exit(1)
                else:
                    print('Download is in progress . waiting for 2 sec')

            #print(export)
            print('Export status before Downloading is: '+export_status)
            #print(export.json())
            if export_status =='Succeeded':
                export_file=requests.get(url_report + '/exports/' + export_id+'/file',
                                      headers={'Authorization': 'Bearer ' + self.access_token,'Content-Type': 'application/json'})
                #with open("Export_Report_File.csv", "w") as outfile:
                 #     outfile.write(export_file)
                open('KPI_Report.csv', 'wb').write(export_file.content)

                print('Report Download has been successfull')
            else:
                print('Report Download has been failed with error:           '+export['error'])

        except Exception as msg:
            log_message = ' Failed - While Downloading the Report '
            log_level = 'ERROR'
            print(log_message, msg, log_level)
            exit(1)
            raise
