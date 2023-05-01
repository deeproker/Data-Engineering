from flask import Flask
from flask import render_template, request, redirect, url_for, flash
app = Flask(__name__)
import boto3
import time
import pandas as pd
import numpy as np
import requests
import json

@app.route('/')
@app.route('/hello')
def Helloworld():
    return "hello world"

@app.route('/forumid',methods=['GET'])
def run_sql():
    client = boto3.client("redshift-data")

    sql="select * from forum where forumid ="+request.args.get('forumid',default='1')#requests.args.get('forumid',default=1)
    ### IAM Restricted Secrets Manager - With Access ###
    res = client.execute_statement(ClusterIdentifier="redshift-cluster-1", \
                                   Database="ransomeware", \
                                   SecretArn="arn:aws:secretsmanager:us-east-2:*********:secret:poc/redshift/cluster1-****", \
                                   Sql=sql)

    query_id = res["Id"]
    done = False
    while not done:
        time.sleep(1)
        status_description = client.describe_statement(Id=query_id)
        status = status_description["Status"]
        if status == "FAILED":
            raise Exception('SQL query failed:' + query_id + ": " + status_description["Error"])
        elif status == "FINISHED":
            if status_description['ResultRows']>0:
                results = client.get_statement_result(Id=query_id)
                column_labels = []
                for i in range(len(results["ColumnMetadata"])): column_labels.append(results["ColumnMetadata"][i]['label'])
                records = []
                for record in results.get('Records'):
                    records.append([list(rec.values())[0] for rec in record])
                df = pd.DataFrame(np.array(records), columns=column_labels)
                return df.to_json()
            else:
                return query_id



@app.route('/forumpost',methods=['GET'])
def run_athena_sql():
    client = boto3.client('athena')

    sql="select forumpostormessagecontent from  ransomeware.forumpostormessages where forumpostormessageid="+request.args.get('messageid',default='3602849')#requests.args.get('forumid',default=1)
    ### IAM Restricted Secrets Manager - With Access ###
    res = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "AwsDataCatalog.ransomeware"},
        ResultConfiguration={
            "OutputLocation": 's3://ransomeware-data-poc/AthenaRestCall/',
        },
    )

    query_id = res["QueryExecutionId"]
    done = False
    while not done:
        status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if status == 'QUEUED' or status == 'RUNNING':
            done = False
            # time.sleep(1)
            status = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        elif status == 'FAILED' or status == 'CANCELLED':
            raise Exception('SQL query failed or cancelled')
        elif status == 'SUCCEEDED':
            result = client.get_query_results(QueryExecutionId=query_id)['ResultSet']['Rows']
            records = []
            column_labels = []
            for cnt in range(0, len(result)):
                if cnt == 0:
                    for i in result[0]['Data']:
                        column_labels.append(i['VarCharValue'])
                else:
                    for i in result[cnt]['Data']:
                        if len(i) > 0:
                            records.append(list(i.values())[0])
                        else:
                            records.append('')
            done = True
            l = zip(column_labels, records)
            return dict(l)

if __name__=='__main__':
    app.debug=True
    app.run(host = '0.0.0.0',port = 5000)
