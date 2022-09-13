import json
from operator import index
from turtle import update
import psycopg2
import base64

array = []
# 1.程式基於使用者輸入的檔案路徑與名稱，載入使用者指定的json設定檔
with open('User.json', encoding="utf-8") as Json_file:
    data = json.load(Json_file)
# 2.程式基於json設定檔中的db資訊進行連線
conn = psycopg2.connect(database=data["my_db"]["database"],user=data["my_db"]["user"],password=data["my_db"]["password"],host=data["my_db"]["host"],port=data["my_db"]["port"])
cur = conn.cursor()

# 3.程式基於json設定檔中的target_tables逐一select出資料表所有資料(table_name)
cur.execute("SELECT * FROM public.\"user\";")
columns = cur.description
rows = cur.fetchall()
selectResult = []
for row in rows:#row列
    append_element = {}# 容器
    for value_index, value in enumerate(row):#value_index順序01234，value值 名字、密碼....
        append_element[columns[value_index].name] = value
    selectResult.append(append_element)
# 4.針對select出來的資料跑迴圈，針對指定clear_columns的原始值進行去機敏處理，然後組成Update SQL字串
updateSql = ""
for row in selectResult:
    setString = ""
    whereString = ""
    for index, eachKey in enumerate(row):
        if len(row) == index+1: #當index等於row的矩陣長度時則成立
            setString = setString + "{eachKey} = '{eachValue}'".format(eachKey=eachKey,eachValue=row[eachKey])
            whereString = whereString + "{eachKey}='{eachValue}'".format(eachKey=eachKey,eachValue=row[eachKey])
        else:
            setString = setString + "{eachKey} = '{eachValue}', ".format(eachKey=eachKey,eachValue=row[eachKey])
            whereString = whereString + "{eachKey}='{eachValue}' and ".format(eachKey=eachKey,eachValue=row[eachKey])
    print(setString)
    print(whereString)
    updateSubSql = "Update public.user\n SET {setString}\n WHERE {whereString};".format(setString = setString,whereString=whereString)
    updateSql = updateSql + updateSubSql
print(updateSql)
# 5.將每筆Update SQL字串整合成一個變數後，放入到execute函式中執行
# cur.execute(updateSql)
# conn.commit()
# conn.close()
