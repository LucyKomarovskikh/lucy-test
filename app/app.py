from flask import Flask, redirect, url_for
import mysql.connector
import requests
import datetime
import pandas


app = Flask(__name__)
DATE_TODAY = datetime.datetime.today()



def connect_db():
    config = {
        'auth_plugin':'mysql_native_password',
        'user': 'root',
        'password': 'root',
        'host': 'db',
        'port': '3306',
        'database': 'vacancies'
    }
    connection = mysql.connector.connect(**config)
    return connection

def get_data_db(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM vacancies_desc where status not like 'closed'")
    data = cursor.fetchall()
    cursor.close()
    return data

def get_data_api():
    date = datetime.datetime(2019, 5, 1)
    data = []
    while date<=DATE_TODAY:
        page_counter = 1
        page = 0
        date_to = date + datetime.timedelta(1)
        while page_counter > 0:
            date_str = date.strftime("%Y-%m-%dT%H:%M:%S+0300")
            date_to_str = date_to.strftime("%Y-%m-%dT%H:%M:%S+0300")
            payload = {'page': page, 'per_page': '100', 'area': '2', 'industry': '7', 'date_from': date_str,'date_to':date_to_str,'Content-Type': 'application/json; charset=UTF-8'}
            r = requests.get("https://api.hh.ru/vacancies", params=payload)
            loaded_json = r.json()
            page_counter = int(loaded_json['pages']) - page
            page = page + 1
            for x in loaded_json['items']:
                row = []
                row.extend([x['id'], date, x['name'],'open'])
                data.append(row)
        date = date_to
    return data

def update_db(ids, connection):
    cursor = connection.cursor()
    for item in ids[0]:
        sql_update = "UPDATE vacancies_desc SET status = 'closed' where id ='" + str(item)+"';"
        cursor.execute(sql_update)
        connection.commit()
    cursor.close()
      

def insert_data(data, connection):
    data_tuples = [tuple(l) for l in data]
    cursor = connection.cursor()
    sql_insert = "INSERT INTO vacancies_desc (id, created, name, status) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql_insert, data_tuples)
    connection.commit()
    cursor.close()
    
def monitor_db(data_db, data_api):
    connection = connect_db()
    new_items = pandas.DataFrame()
    items_to_update = pandas.DataFrame()
    df_data_api = pandas.DataFrame(data_api)
    df_data_db = pandas.DataFrame(data_db)
    df_data_db = df_data_db.loc[~df_data_db[0].duplicated(keep='first')]
    items_to_update = df_data_db[~df_data_db[0].isin(df_data_api[0])]
    new_items = df_data_api[~df_data_api[0].isin(df_data_db[0])]
    new_items_list = new_items.values.tolist()
    update_db(items_to_update, connection)
    insert_data(new_items_list, connection)
    connection.close()
    return len(new_items_list)

def put_it_all_together():
    connection = connect_db()
    if len(get_data_db(connection))!= 0:
        data_from_api = get_data_api()
        data_db = get_data_db(connection)
        n = monitor_db(data_db, data_from_api)
        return 'The database has been updated at '+ str(DATE_TODAY) +' ------ ' + str(n)+' rows were inserted on update'
    else:
        return redirect(url_for('initial_load'))
        
@app.route('/update')
def update():
    return put_it_all_together() 
    

@app.route('/initial_load')
def initial_load():
    connection = connect_db()
    if len(get_data_db(connection)) == 0:
        data_from_api = get_data_api()
        if data_from_api:
            insert_data(data_from_api, connection)
    connection.close()
    return redirect(url_for('update'))

if __name__ == '__main__':
    app.run(host='0.0.0.0')



