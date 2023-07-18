from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient, FileSystemClient
import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import datetime
from isodate import UTC
from numpy import record
import numpy as np
import azure.functions as func
from draftjs_exporter.html import HTML
import re

############################################################ SAMPLE CHUCK OF CODE ##########################################################################################

def main(msg: func.ServiceBusMessage):



        
    my_connection_string = ""
    sas_token =''
    my_account_url=""
    my_filesystem = ""
    blob_service_client = BlobServiceClient.from_connection_string(my_connection_string)
    service_client = DataLakeServiceClient(account_url=my_account_url, credential=sas_token)
    file_system_client = service_client.get_file_system_client(file_system=my_filesystem)


    dict_obj = msg.get_body()

    config = {}
    exporter = HTML(config)
    table_got = []
    found_table = []
    INLNE_RESUME_CLOB = ""
    JOB_DESCRIPTION_FROM_CLIENT = ""
    INTERNAL_JOB_DESCRIPTION = "" 

    # blob_client = blob_service_client.get_blob_client(container='staffing-origin-live/staffing-origin-live-incr-landing-zone',blob='loadtxt' + datetime.datetime.utcnow().strftime('%Y-%m-%d') + '.txt')
    # blob_client.upload_blob(dict_obj, overwrite=True)
    
    dict_obj = json.loads(dict_obj)

    dict_obj = dict_obj.replace('null','""')

    dict_obj = json.loads(dict_obj)
    
    dict_obj_keys = dict_obj.keys()
    print(dict_obj_keys)
    for table in dict_obj:
        if table == "CANDIDATE":
            for col_no in range(len(dict_obj['CANDIDATE'])):
                data = json.loads('{}'.format(dict_obj['CANDIDATE'][col_no-1]['INLNE_RESUME_CLOB']))
                for i in data['blocks']:
                    key, text, type, depth = i['key'], i['text'], i['type'], i['depth']
                    inlineStyleRanges, entityRanges, data = i['inlineStyleRanges'], i['entityRanges'], i['data']
                    html = exporter.render({'blocks': [{'key': key, 'text': text, 'type': type,'depth': 0,'inlineStyleRanges': [],'entityRanges': [],'data': data}], 'entityMap': {}})
                    INLNE_RESUME_CLOB = INLNE_RESUME_CLOB + html
                    INLNE_RESUME_CLOB = ''.join([i if ord(i) < 128 else '' for i in INLNE_RESUME_CLOB])
            dict_obj['CANDIDATE'][0].update({'INLNE_RESUME_CLOB':INLNE_RESUME_CLOB})
            dict_obj['CANDIDATE'][0].update({'INLNE_RESUME_CLOB':dict_obj['CANDIDATE'][0]['INLNE_RESUME_CLOB'].replace('\n','<br>')})
            dict_obj['CANDIDATE'][0].update({'INLNE_RESUME_CLOB':dict_obj['CANDIDATE'][0]['INLNE_RESUME_CLOB'].replace('\\x','&#x').replace('"','\'').replace('/<!--.*?-->/g', '')})


        if table == "REQUIREMENT":
            for col_no in range(len(dict_obj['REQUIREMENT'])):
                data_jdfc = json.loads('{}'.format(dict_obj['REQUIREMENT'][col_no]['JOB_DESCRIPTION_FROM_CLIENT'])) 
                data_ijd = json.loads('{}'.format(dict_obj['REQUIREMENT'][col_no]['INTERNAL_JOB_DESCRIPTION']))
                
                for i in data_jdfc['blocks']:
                    key, text, type, depth = i['key'], i['text'], i['type'], i['depth']
                    inlineStyleRanges, entityRanges, data = i['inlineStyleRanges'], i['entityRanges'], i['data']
                    html = exporter.render({'entityMap': {},'blocks': [{'key': key, 'text': text, 
                    'type': type,'depth': depth,'inlineStyleRanges': [],'entityRanges': entityRanges,'data': data}]})
                    JOB_DESCRIPTION_FROM_CLIENT = JOB_DESCRIPTION_FROM_CLIENT + html
                    JOB_DESCRIPTION_FROM_CLIENT = ''.join([i if ord(i) < 128 else '' for i in JOB_DESCRIPTION_FROM_CLIENT])
                    dict_obj['REQUIREMENT'][col_no].update({'JOB_DESCRIPTION_FROM_CLIENT':JOB_DESCRIPTION_FROM_CLIENT})
                    dict_obj['REQUIREMENT'][col_no].update({'JOB_DESCRIPTION_FROM_CLIENT':dict_obj['REQUIREMENT'][col_no]['JOB_DESCRIPTION_FROM_CLIENT'].replace('\n','<br>')})
                    dict_obj['REQUIREMENT'][col_no].update({'JOB_DESCRIPTION_FROM_CLIENT':dict_obj['REQUIREMENT'][col_no]['JOB_DESCRIPTION_FROM_CLIENT'].replace('"','\'').replace('/<!--.*?-->/g', '')})

                for i in data_ijd['blocks']:
                    key, text, type, depth = i['key'], i['text'], i['type'], i['depth']
                    inlineStyleRanges, entityRanges, data = i['inlineStyleRanges'], i['entityRanges'], i['data']
                    html = exporter.render({'entityMap': {},'blocks': [{'key': key, 'text': text, 
                    'type': type,'depth': depth,'inlineStyleRanges': [],'entityRanges': entityRanges,'data': data}]})
                    INTERNAL_JOB_DESCRIPTION = INTERNAL_JOB_DESCRIPTION + html 
                    INTERNAL_JOB_DESCRIPTION = ''.join([i if ord(i) < 128 else '' for i in INTERNAL_JOB_DESCRIPTION])
                    dict_obj['REQUIREMENT'][col_no].update({'INTERNAL_JOB_DESCRIPTION':INTERNAL_JOB_DESCRIPTION})
                    dict_obj['REQUIREMENT'][col_no].update({'INTERNAL_JOB_DESCRIPTION':dict_obj['REQUIREMENT'][col_no]['INTERNAL_JOB_DESCRIPTION'].replace('\n','<br>')})
                    dict_obj['REQUIREMENT'][col_no].update({'INTERNAL_JOB_DESCRIPTION':dict_obj['REQUIREMENT'][col_no]['INTERNAL_JOB_DESCRIPTION'].replace('"','\'').replace('/<!--.*?-->/g', '')})

        table_name = table
        if(str(table_name)=="AE_TRANSACTION_ID"):
            aetid = format(dict_obj.get(table_name))
            continue
        if(str(table_name)=="AE_TIMESTAMP_TS"):
            aetts = format(dict_obj.get(table_name))
            continue
        tblnm1 = 'et_' + (str(table_name)).lower()
        json_results = format(dict_obj.get(table_name))
        i = json_results.find('[')+1
        json_results_new=''
        while i < (len(json_results)-1):
            if(json_results[i] != '}'):
                    json_results_new = json_results_new + json_results[i]
                    json_results_new = re.sub(r'','',json_results_new)
                    json_results_new = re.sub(r"': None",r':""',json_results_new)
                    json_results_new = re.sub("{'",r'{"',json_results_new)
                    json_results_new = re.sub(r"':\s'",r'": "',json_results_new)
                    json_results_new = re.sub(r"',\s'",r'", "',json_results_new)
                    json_results_new = re.sub(r",\s'",r', "',json_results_new)
                    json_results_new = re.sub(r"':'",r'": "',json_results_new)
                    json_results_new = re.sub(r"':\s", r'": ',json_results_new)
                    json_results_new = re.sub(r"','",r'", "',json_results_new)
                    json_results_new = re.sub(r"':\s\"",r'": "',json_results_new)
                    json_results_new = re.sub(r'",\s\'',r'", "',json_results_new)
                    json_results_new = re.sub(r'\\x','&#x',json_results_new)
                    json_results_new = re.sub(r'\\u','&#u',json_results_new)
                    json_results_new = re.sub(r": '",r': "',json_results_new)
                    json_results_new = re.sub("'}",r'"}',json_results_new)                
                
                    i += 1
            else:
                    json_results_new = json_results_new + '}' + '\n'
                    json_results_new = re.sub(r'','',json_results_new)
                    json_results_new = re.sub(r"': None",r':""',json_results_new)
                    json_results_new = re.sub("{'",r'{"',json_results_new)
                    json_results_new = re.sub(r"':\s'",r'": "',json_results_new)
                    json_results_new = re.sub(r"',\s'",r'", "',json_results_new)
                    json_results_new = re.sub(r",\s'",r', "',json_results_new)
                    json_results_new = re.sub(r"':'",r'": "',json_results_new)
                    json_results_new = re.sub(r"':\s", r'": ',json_results_new)
                    json_results_new = re.sub(r"','",r'", "',json_results_new)
                    json_results_new = re.sub(r"':\s\"",r'": "',json_results_new)
                    json_results_new = re.sub(r'",\s\'',r'", "',json_results_new)
                    json_results_new = re.sub(r'\\x','&#x',json_results_new)
                    json_results_new = re.sub(r'\\u','&#u',json_results_new)
                    json_results_new = re.sub(r": '",r': "',json_results_new)
                    json_results_new = re.sub("'}",r'"}',json_results_new)                

                    i += 2    
        blob_client = blob_service_client.get_blob_client(container='staffing-origin-live/staffing-origin-live-raw', blob=  tblnm1 + '/' + datetime.datetime.utcnow().strftime('%Y') + '/' + datetime.datetime.utcnow().strftime('%m') + '/' + datetime.datetime.utcnow().strftime('%d') + '/' + tblnm1 + '_' + datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S-%p') + '.json')
        blob_client.upload_blob(json_results_new, overwrite=True)
            
        blob_list = blob_service_client.get_container_client('staffing-origin-live')
        folder_names_adls_raw = []
        blob_list = blob_list.list_blobs(name_starts_with="staffing-origin-live-raw/")
        for blob in blob_list:
            sp = blob.name.find("/") +1
            blobname = blob.name[sp:len(blob.name)]
            if (blobname.find("/") == -1):
                folder_names_adls_raw.append(blobname)
                #print(blobname)
        list = ['']


        if tblnm1 in list:
                
            if(tblnm1 =='et_association'):

                paths = file_system_client.get_paths(path="staffing-origin-live-raw/et_association")
                path_prefix = ""
                file_paths = []
                for path in paths:
                    if (path.name.endswith("json")):
                        file_name = path_prefix + path.name + '?' + sas_token
                        file_paths.append(file_name)

                ASSOCIATION_TABLE_DF = pd.concat([pd.read_json(f, orient="records",lines=True) for f in file_paths])
                ASSOCIATION_TABLE_DF = ASSOCIATION_TABLE_DF.astype({'COUNTRY_UUID':'string'})
                #print(list(ASSOCIATION_TABLE_DF))
                #print(ASSOCIATION_TABLE_DF.count()) 
                #print(ASSOCIATION_TABLE_DF.head())


                # READING APPLICATION_USER_MSTER


                paths = file_system_client.get_paths(path="staffing-origin-live-raw/et_application_user_master")
                path_prefix = ""
                file_paths = []
                for path in paths:
                    if (path.name.endswith("json")):
                        file_name = path_prefix + path.name + '?' + sas_token
                        file_paths.append(file_name)

                APPLICATION_USER_MSTER = pd.concat([pd.read_json(f, orient="records",lines=True) for f in file_paths])
                APPLICATION_USER_MSTER_DF=APPLICATION_USER_MSTER[["FIRSTNAME","LASTNAME","APPLICATION_USERID"]]
                # print(APPLICATION_USER_MSTER_DF) 
                # print(APPLICATION_USER_MSTER_DF.head())



                #READING ASSOCIATION_STAGE_MSTER


                paths = file_system_client.get_paths(path="staffing-origin-live-raw/et_association_stage_master")
                path_prefix = ""
                file_paths = []
                for path in paths:
                    if (path.name.endswith("json")):
                        file_name = path_prefix + path.name + '?' + sas_token
                        file_paths.append(file_name)

                ASSOCIATION_STAGE_MSTER = pd.concat([pd.read_json(f, orient="records",lines=True) for f in file_paths])
                ASSOCIATION_STAGE_MSTER_DF=ASSOCIATION_STAGE_MSTER[["ASSOCIATION_STAGE_CD","ASSOCIATION_STAGE_UUID"]]
                ASSOCIATION_STAGE_MSTER_DF.rename(columns = {'ASSOCIATION_STAGE_CD':'ASSOCIATION_STAGE_D'}, inplace = True)
                # print(list(ASSOCIATION_STAGE_MSTER_DF))
                # print(APPLICATION_USER_MSTER_DF) 
                # print(ASSOCIATION_STAGE_MSTER_DF)


                # READING ASSOCIATION_STATUS_MSTER


                paths = file_system_client.get_paths(path="staffing-origin-live-raw/et_association_status_master")
                path_prefix = ""
                file_paths = []
                for path in paths:
                    if (path.name.endswith("json")):
                        file_name = path_prefix + path.name + '?' + sas_token
                        file_paths.append(file_name)

                ASSOCIATION_STATUS_MSTER = pd.concat([pd.read_json(f, orient="records",lines=True) for f in file_paths])
                ASSOCIATION_STATUS_MSTER_DF=ASSOCIATION_STATUS_MSTER[["ASSOCIATION_STATUS","ASSOCIATION_STATUS_UUID"]]
                ASSOCIATION_STATUS_MSTER_DF.rename(columns = {'ASSOCIATION_STATUS':'ASSOCIATION_STATUS_D'}, inplace = True)
                # print(ASSOCIATION_STAGE_MSTER_DF) 
                # print(ASSOCIATION_STATUS_MSTER_DF.head())

                #READING COUNTRY_MSTER

                paths = file_system_client.get_paths(path="staffing-origin-live-raw/et_country_master")
                path_prefix = ""
                file_paths = []
                for path in paths:
                    if (path.name.endswith("json")):
                        file_name = path_prefix + path.name + '?' + sas_token
                        file_paths.append(file_name)
                COUNTRY_MSTER = pd.concat([pd.read_json(f, orient="records",lines=True) for f in file_paths])
                COUNTRY_MSTER_DF=COUNTRY_MSTER[["COUNTRY_NAME", "COUNTRY_UUID"]]
                COUNTRY_MSTER_DF = COUNTRY_MSTER_DF.astype({'COUNTRY_UUID':'string'})
                COUNTRY_MSTER_DF.rename(columns = {'COUNTRY_UUID':'COUNTRY_UUID_D'}, inplace = True)
                



                #DENORMALIZATION OF ASSOCIATION TABLE

                ASSOCIATION_DF= pd.merge(ASSOCIATION_TABLE_DF,APPLICATION_USER_MSTER_DF,how='left',left_on='AE_INSERT_ID',right_on='APPLICATION_USERID')
                ASSOCIATION_DF["AE_INSERT_ID_NAME"] = ASSOCIATION_DF["FIRSTNAME"] + ' ' + ASSOCIATION_DF["LASTNAME"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=['FIRSTNAME','LASTNAME','APPLICATION_USERID'])

                ASSOCIATION_DF= pd.merge(ASSOCIATION_DF,APPLICATION_USER_MSTER_DF,how='left',left_on='AE_UPDATE_ID',right_on='APPLICATION_USERID')
                ASSOCIATION_DF["AE_UPDATE_ID_NAME"] = ASSOCIATION_DF["FIRSTNAME"] + ' ' + ASSOCIATION_DF["LASTNAME"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=['FIRSTNAME','LASTNAME','APPLICATION_USERID'])

                ASSOCIATION_DF= pd.merge(ASSOCIATION_DF,APPLICATION_USER_MSTER_DF,how='left',left_on='ASSOCIATION_UPDATED_BY',right_on='APPLICATION_USERID')
                ASSOCIATION_DF["ASSOCIATION_UPDATED_BY_NAME"] = ASSOCIATION_DF["FIRSTNAME"] +' '+ ASSOCIATION_DF["LASTNAME"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=['FIRSTNAME','LASTNAME','APPLICATION_USERID'])

                ASSOCIATION_DF= pd.merge(ASSOCIATION_DF,APPLICATION_USER_MSTER_DF,how='left',left_on='ASSOCIATION_CREATED_BY',right_on='APPLICATION_USERID')
                ASSOCIATION_DF["ASSOCIATION_CREATED_BY_NAME"] = ASSOCIATION_DF["FIRSTNAME"] +' '+ ASSOCIATION_DF["LASTNAME"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=['FIRSTNAME','LASTNAME','APPLICATION_USERID'])

                ASSOCIATION_DF= pd.merge(ASSOCIATION_DF,ASSOCIATION_STAGE_MSTER_DF,how='left',left_on='ASSOCIATION_STAGE',right_on='ASSOCIATION_STAGE_UUID')
                ASSOCIATION_DF["ASSOCIATION_STAGE_DESC"] = ASSOCIATION_DF["ASSOCIATION_STAGE_D"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=["ASSOCIATION_STAGE_UUID","ASSOCIATION_STAGE_D"])

                ASSOCIATION_DF= pd.merge(ASSOCIATION_DF,ASSOCIATION_STATUS_MSTER_DF,how='left',left_on='ASSOCIATION_STATUS',right_on='ASSOCIATION_STATUS_UUID')
                ASSOCIATION_DF["ASSOCIATION_STATUS_DESC"] = ASSOCIATION_DF["ASSOCIATION_STATUS_D"]
                ASSOCIATION_DF=ASSOCIATION_DF.drop(columns=["ASSOCIATION_STATUS_UUID","ASSOCIATION_STATUS_D"])

                ASSOCIATION_DF = pd.merge(ASSOCIATION_DF,COUNTRY_MSTER_DF,how = 'left',left_on='COUNTRY_UUID',right_on="COUNTRY_UUID_D")
                ASSOCIATION_DF["COUNTRY_DESC"] = ASSOCIATION_DF["COUNTRY_NAME"]
                ASSOCIATION_DF = ASSOCIATION_DF.drop(columns=["COUNTRY_NAME", "COUNTRY_UUID_D"])


                # print(ASSOCIATION_DF)


                # #WRITING INTO AZURE

                FINALRESULT =ASSOCIATION_DF.to_json(orient='records')
                i = FINALRESULT.find('[')+1
                json_results_new=''
                while i < (len(FINALRESULT)-1):
                    if(FINALRESULT[i] != '}'):
                            json_results_new = json_results_new + FINALRESULT[i]
                            i += 1
                    else:
                            json_results_new = json_results_new + '}' + '\n'
                            i += 2
                FINALRESULT = json_results_new
                blob_client = blob_service_client.get_blob_client(container='staffing-origin-live/staffing-origin-live-processed', blob =  'et_association' + '/' + 'et_association' + '_' + datetime.datetime.utcnow().strftime('%Y-%U') + '.json')
                blob_client.upload_blob(FINALRESULT, overwrite=True)
                blob_client = blob_service_client.get_blob_client(container='staffing-origin-live/staffing-origin-live-processed', blob =  'et_association_audit' + '/' + 'et_association_audit' + '_' + datetime.datetime.utcnow().strftime('%Y-%U') + '.json')
                blob_client.upload_blob(FINALRESULT, overwrite=True)
                print("Uploaded Successfully")
