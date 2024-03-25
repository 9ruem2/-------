import json
import boto3
import botocore
from urllib.parse import unquote_plus
from cherrypicker import CherryPicker
import parsing
import send_db
import query
import request_api
import pandas as pd
import time


client = boto3.client('s3')
s3 = boto3.client("s3")
S3_BUCKET = 'avalve'

#main
def lambda_handler(event, context):
    
    json_filename = event["headers"]["json_filename"]
    json_year = json_filename[0:4]
    json_month = json_filename[5:7]
    json_day = json_filename[8:10]
    object_key = "Smartfarm/seoulfnf/yongin_camera/"+json_year+"/"+json_month+json_day+"/json/"+json_filename
    file_content = s3.get_object(Bucket=S3_BUCKET, Key=object_key)["Body"].read().decode('utf-8')
    data = json.loads(file_content) 
    
    
    # json파일 변환 -> pandas
    picker = CherryPicker(data)
    
    #현재 람다와 연결된 스마트팜 uuid 구하기
    uuid = picker['device_uuid'].get()
    
    #S3 이미지 주소 & 람다 실행 시간대 구하기
    mea_time = picker['measure_session_start'].get()
    year = mea_time[0:4]
    day = mea_time[5:7]+mea_time[8:10]
    check_time = mea_time[11:13]
    
    
    #측정 시간 정보 DB 저장
    result1 = parsing.measurement_sessions(picker)
    send_db.send_db(result1,'MEASUREMENT_SESSIONS','append')
    
    #센서 정보 구하고 DB 저장은 1회만, 내용 변경시 대체 하도록 수정 작업 필요
    result2 = parsing.sensors(picker,uuid)
    # send_db.send_db(result2,'SENSORS','append')
    
    #session_index 값 구하기
    row = query.session_index(uuid)
    
    #환경 값, 1층 이미지, 2층 이미지 구하기
    # result3 = parsing.sensor_measurements_env(picker, result2, row)
    # result4 = parsing.sensor_measurements_1f(picker, result2, row)
    result5 = parsing.sensor_measurements_2f(picker, result2, row)
    
    sort = result5['measured_value']
    json_data = sort.to_json()
    try:
        inter_result = request_api.get_result(json_data)
        
        
        df = pd.read_json(inter_result)
        filter_list = df['plant_exist'].tolist()
        name_list = df['plant_name'].tolist()
        weight_list = df['weight'].tolist()
        tipburn_list = df['tipburn'].tolist()
        
        
        result5['weight'] = weight_list
        result5['plant_exist'] = filter_list
        result5['tipburn'] = tipburn_list
        result5['plant_name'] = name_list
        
        # # 위의 값 합쳐서 DB 저장
        # result_con = pd.concat([result3,result5])
        
        send_db.send_db(result5,'SENSOR_MEASUREMENTS','append')
    
    except Exception as error:
        # API 호출 실패 시, 첫 번째 DB 저장 작업 롤백
        print(f"API call failed: {error}. Rolling back database entry.")
        query.delete(row)
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to process data and rolled back initial database entry')
        }
    
    # 성공적으로 모든 처리가 완료되었을 때
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed data')
    }