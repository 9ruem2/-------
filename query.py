import pymysql
import os

database_username= os.environ['database_username']
database_password= os.environ['database_password']
database_ip= os.environ['database_ip']
database_name= os.environ['database_name']

def session_index(device_uuid):
    # MySQL Connection 연결
    conn = pymysql.connect(host=database_ip, user=database_username, password= database_password,
                           db=database_name, charset='utf8')
     
    # Connection 으로부터 Cursor 생성
    curs = conn.cursor()
    
    
   
    # SQL문 실행
    sql = "select `index` from `MEASUREMENT_SESSIONS` where `device_uuid` = %s order by `index` desc limit 1"
    
    
    curs.execute(sql,device_uuid)
     
    # 데이타 Fetch
    rows = curs.fetchall()
   
    
    # Connection 닫기
    conn.close()
    return rows[0][0]

def measurement_index(session_index):
    # MySQL Connection 연결
    conn = pymysql.connect(host=database_ip, user=database_username, password= database_password,
                           db=database_name, charset='utf8')
     
    # Connection 으로부터 Cursor 생성
    curs = conn.cursor()
    
    # SQL문 실행
    sql = "select `index` from `SENSOR_MEASUREMENTS` where `session_index` = %s and `sensor_index` > 3 order by `index` desc"
    
    curs.execute(sql,(session_index))
     
    # 데이타 Fetch
    rows = curs.fetchall()
    new = []
    for f in range(0, len(rows)):
        new.append(rows[f][0])
    new.reverse()
    # Connection 닫기
    conn.close()
    return new

def insert_db(session_index, measurement_time, daily_yield):
    conn = pymysql.connect(host=database_ip, user=database_username, password= database_password,
                           db=database_name, charset='utf8')
    
    curs = conn.cursor()

    sql = """INSERT INTO PLANTS_ANALYSIS_DAY(session_index, measurement_time, daily_yield) VALUES (%s, %s, %s)"""

    curs.execute(sql,(session_index, measurement_time, daily_yield))
    conn.commit()
    conn.close()



def delete(session_index):
   
    # MySQL Connection 연결
    conn = pymysql.connect(host=database_ip, user=database_username, password=database_password,
                           db=database_name, charset='utf8')
    
    try:
        # Connection 으로부터 Cursor 생성
        curs = conn.cursor()

        # session_index가 일치하는 항목을 삭제하는 SQL 쿼리 실행
        sql = "DELETE FROM MEASUREMENT_SESSIONS WHERE `index` = %s"
        curs.execute(sql, (session_index,))

        # 변경 사항 저장
        conn.commit()
        print(f"Deleted session with session_index: {session_index}")
    except Exception as e:
        # 에러 발생 시, 에러 메시지 출력
        print(f"Error occurred: {e}")
    finally:
        # Connection 닫기
        conn.close()

# 함수 사용 예시
# session_index, database_ip, database_username, database_password, database_name 변수에 적절한 값을 할당 후 아래 함수 호출
# delete_measurement_session_by_session_index(session_index, database_ip, database_username, database_password, database_name)
