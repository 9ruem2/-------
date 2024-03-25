import requests
import pandas as pd



def do_filtering(data):
    headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
    }
    url = "http://43.201.73.9:8000/natural_everything_filter"
    response = requests.post(url, headers=headers,data=data)
    json_data = response.json()
    df = pd.read_json(json_data)
    filter_list = df['plant_exist'].tolist()
    
    return filter_list

def get_result(data):
    headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
    }
    url = "http://13.125.59.221:8000/natural_everything"
    response = requests.post(url, headers=headers,data=data)
    json_data = response.json()
    
    
    return json_data

def get_classify(year, day, inv):
    list_1 = []
    
    for n in range(len(inv)):        
        data = ("https://avalve.s3.ap-northeast-2.amazonaws.com/Smartfarm/Andong/andong_container/"
        +year
        +"/" 
        +day
        +"/image/"
        +inv["measured_value"][n])
        try:
            response1=requests.post("http://14.49.44.133:8000/classify",headers={"content-type": "application/json"},data = data)
            list_1.append(int(response1.text)+1)
        except:
            list_1.append("")
           
    return list_1
    
def get_predictHarvestTime(inv):
    list_1 = []
    
    for n in range(len(inv)):
        if type(inv[n]) is int:
            response1=requests.post("http://14.49.44.133:8000/predictHarvestTime",headers={"content-type": "application/json"}, data = (str(inv[n])))
            list_1.append(response1.text)
        else:
            list_1.append("")
            
            
    return list_1

def get_predictCropsYield(inv):
    list_1 = []
    
    for n in range(len(inv)):
        if type(inv[n]) is int:
            response1=requests.post("http://14.49.44.133:8000/predictCropsYield",headers={"content-type": "application/json"}, data = (str(inv[n])))
            list_1.append(response1.text)
        else:
            list_1.append("")
            
    return list_1