import datetime
import pandas as pd
import numpy as np
import sqlalchemy
from cherrypicker import CherryPicker

def measurement_sessions(picker):
    
    config = picker['device_uuid','measure_session_start', 'end_time'].get()
    
    df = pd.DataFrame(config)
    df = df.transpose()
    df.columns = ['device_uuid','session_start_datetime', 'session_end_datetime']
    return df
    
def sensors(picker,uuid):
    
    config = picker['sensors'].get()

    df = pd.DataFrame(config)
    df = df.transpose()
    df.insert(0,'index_per_device', df.index, allow_duplicates=False)
    df.insert(0,'device_uuid', uuid, allow_duplicates=False)
    df.columns = ['device_uuid','index_per_device','sensor_model_no','measure_brief_txt','unit','sensor_position','comment']
    
    
    return df
    
def sensor_measurements_env(picker, position, session_index):
    
    config = picker['measure_sensor_hub'].get()
    
    df = pd.DataFrame(config, index = ["measured_value"])
    df = df.transpose()
    df.insert(1,'sensor_position', position["sensor_position"], allow_duplicates=False)
    df.insert(0,'sensor_index', df.index, allow_duplicates=False)
    df.insert(0,'session_index', len(df)*[session_index], allow_duplicates=False)
    
    return df

def sensor_measurements_1f(picker, position, session_index):
    
    config = picker['measure_carriage0'].get()
    df = pd.DataFrame(config)
    df_rename = df.rename(columns={'value':'measured_value'})
    df_rename.insert(0,'session_index', len(df)*[session_index], allow_duplicates=False)
    
    return df_rename

def sensor_measurements_2f(picker, position, session_index):
    
    config = picker['measure_camera'].get()
    test = {item['sensor_index']: item['value'] for item in config}
    df = pd.DataFrame(test,index = ["measured_value"])
    df = df.transpose()
    df.insert(1,'sensor_position', position["sensor_position"], allow_duplicates=False)
    df.insert(0,'sensor_index', df.index, allow_duplicates=False)
    df.insert(0,'session_index', len(df)*[session_index], allow_duplicates=False)
    df.reset_index(drop=True)
    
    return df




