import json
import psycopg2
import base64

# 1.程式基於使用者輸入的檔案路徑與名稱，載入使用者指定的json設定檔
with open('User.json', encoding="utf-8") as Json_file:
    data = json.load(Json_file)
# 2.程式基於json設定檔中的db資訊進行連線
conn = psycopg2.connect(database=data["my_db"]["database"],user=data["my_db"]["user"],password=data["my_db"]["password"],host=data["my_db"]["host"],port=data["my_db"]["port"])
cur = conn.cursor()
# 3.程式基於json設定檔中的target_tables逐一select出資料表所有資料(table_name)


# 4.針對select出來的資料跑迴圈，針對指定clear_columns的原始值進行去機敏處理，然後組成Update SQL字串
total_sql = ""
for each in data["user"]["userdata"]:
    sql ="""
    INSERT INTO public."user"(name, password, gender, height, weight) VALUES('{name}','{password}','{gender}',{height},{weight});
    """.format(
        name = each["name"],
        password = each["password"],
        gender  = each["gender"],
        height = each["height"],
        weight = each["weight"]
    )
    total_sql = total_sql + sql
cur.execute(total_sql)
conn.commit()
print ("Records created successfully")
conn.close()
