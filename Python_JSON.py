import json
from operator import index
from turtle import update
import psycopg2
import base64
import hashlib
# 1.程式基於使用者輸入的檔案路徑與名稱，載入使用者指定的json設定檔
filename=input("請輸入JSON檔名:")
print("歡迎使用,"+filename)
with open(filename, encoding="utf-8") as Json_file:
    data = json.load(Json_file)
# 2.程式基於json設定檔中的db資訊進行連線
conn = psycopg2.connect(database=data["db"]["database"],user=data["db"]["user"],password=data["db"]["password"],host=data["db"]["host"],port=data["db"]["port"])
cur = conn.cursor()
# 3.程式基於json設定檔中的target_tables逐一select出資料表所有資料(table_name)
for eachTable in data["target_tables"]:
    selectsql="SELECT * FROM {table_name};".format(table_name =eachTable["table_name"])
    cur.execute(selectsql)
    columns = cur.description
    rows = cur.fetchall()
    selectResult = []
    for row in rows:#row列
        append_element = {}#容器
        for value_index, value in enumerate(row):#value_index順序01234，value值 名字、密碼....
            append_element[columns[value_index].name] = value
        selectResult.append(append_element)
    # 4.針對select出來的資料跑迴圈，針對指定clear_columns的原始值進行去機敏處理，然後組成Update SQL字串
    updateSql = ""
    for row in selectResult:
        setString = ""
        whereString = ""
        for index, eachKey in enumerate(row):
            raw_data = row[eachKey]
            convert_data = row[eachKey]
            if eachKey in eachTable["clear_columns"]:
                #convert_data = str(base64.b64encode(convert_data.encode()))
                convert_data = str(hashlib.md5(convert_data.encode()).hexdigest())
            if len(row) == index+1: #當index等於row的矩陣長度時則成立
                setString = setString + "{eachKey} = '{eachValue}'".format(eachKey=eachKey,eachValue=convert_data)
                whereString = whereString + "{eachKey}='{eachValue}'".format(eachKey=eachKey,eachValue=raw_data)
            else:
                setString = setString + "{eachKey} = '{eachValue}', ".format(eachKey=eachKey,eachValue=convert_data)
                whereString = whereString + "{eachKey}='{eachValue}' and ".format(eachKey=eachKey,eachValue=raw_data)
        print(setString)
        print(whereString)
        updateSubSql = "Update {user} SET {setString} WHERE {whereString};".format(user=eachTable["table_name"],setString = setString,whereString=whereString)
        updateSql = updateSql + updateSubSql
print(updateSql)
# 5.將每筆Update SQL字串整合成一個變數後，放入到execute函式中執行
cur.execute(updateSql)
conn.commit()
conn.close()