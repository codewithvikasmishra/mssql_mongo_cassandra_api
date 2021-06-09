# conda create -n name python==3.6.9
# conda create -n sudh1231243 python==3.6.9
# conda activate sudh1231243
# jupyter notebook
from typing import Collection
from flask import Flask, request, jsonify,abort, after_this_request
import tempfile
import os
import subprocess
import json
from flask.ctx import after_this_request
import pandas as pd
from connection.db_connect_sql import *
from config.logger import *

logger = logging.getLogger('mssql')
setup_logger(logger,'logs/sql/mssql.logs')

A=Cnxn
sql_cnxn=A.sql_conn()

app = Flask(__name__)

@app.route('/sql/select', methods = ['POST'])
def sql_select():
    result=[]
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    cursor=sql_cnxn.cursor()
    try:
        cursor.execute(query['qry'])
        for row in cursor:
            result.append(list(row))
        logger.info("Results appended in result list for SQL select")
    except Exception as e:
        logger.error(f"Please check the query in SQL select.{e}")
        return abort(400, "Please verify SQL query")
    return json.dumps(result)

@app.route('/sql/create', methods = ['POST'])
def sql_create():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    cursor=sql_cnxn.cursor()
    try:
        cursor.execute(query['qry'])
        sql_cnxn.commit()
        logger.info("Table has been created for SQL create")
        return "table created"
    except Exception as e:
        logger.error(f"error in query for SQL create.{e}")
        return abort(400, "Please verify SQL query/ \n Given table is already available")


@app.route('/sql/insert', methods = ['POST'])
def sql_insert():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    cursor=sql_cnxn.cursor()
    try:
        cursor.execute(query['qry'])
        sql_cnxn.commit()
        logger.info("records has been inserted for SQL insert")
        return "records inserted"
    except Exception as e:
        logger.error(f"error in query for SQL insert.{e}")
        return abort(400, "Please verify SQL query")

@app.route('/sql/update', methods = ['POST'])
def sql_update():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    cursor=sql_cnxn.cursor()
    try:    
        cursor.execute(query['qry'])
        sql_cnxn.commit()
        logger.info("records has been updated for SQL update")
        return "records updated"
    except Exception as e:
        logger.error(f"error in query for SQL update.{e}")
        return abort(400, "Please verify SQL query \n given table is not present in database")


@app.route('/sql/bulk_insert', methods = ['POST'])
def sql_bulk_insert():
    db_table=request.form.get('db_table')
    csv_file=request.files.get('file')
    if csv_file.filename == '':
        return abort(400, "File not attached in body. Please attach csv document.")
    elif db_table=='':
        return abort(400, "Please fill the db name.")

    temp_folder = tempfile.TemporaryDirectory()
    file_name = csv_file.filename
    is_file = True
    file_path = os.path.join(temp_folder.name, file_name)
    csv_file.save(file_path)

    query ="BULK INSERT " + db_table + " FROM '" + file_path +"' WITH (FORMAT='CSV', FIRSTROW=2)"
    cursor=sql_cnxn.cursor()
    try:
        os.chmod(temp_folder.name,0o77)
        cursor.execute(query)
        sql_cnxn.commit()
        logger.info("records has been inserted for SQL bulk-insert")
        return "Bulk Insert completed from csv file"
    except Exception as e:
        logger.error(f"error in query for SQL bulk-insert.{e}")
        return abort(400, "Please verify SQL query \n given table is not present in database \n Attached file is corrupted")

    @after_this_request
    def cleanup(folder):
        temp_folder.cleanup()
        return folder

@app.route('/sql/delete', methods = ['POST'])
def sql_bulk_delete():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        cursor=sql_cnxn.cursor()
        cursor.execute(query['qry'])
        sql_cnxn.commit()
        logger.info("records has been deleted for SQL delete")
        return "records deleted"
    except Exception as e:
        logger.error(f"error in query for SQL delete.{e}")
        return abort(400, "Please verify SQL query \n given table is not present in database")

@app.route('/sql/download', methods = ['POST'])
def sql_bulk_download():
    query = request.get_json(['qry'])
    if list(request.get_json().keys()).sort()!=['qry', 'file_name'].sort():
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        df=pd.DataFrame(pd.read_sql_query(query['qry'],sql_cnxn))
        df.to_csv(r'/home/vikesh/Documents/ineuron/db_api/csv/download_API/'+query['file_name'])
        logger.info("records has been downloaded in csv file for SQL download")
        return ("table downloded in given path")
    except Exception as e:
        logger.error(f"either query is wrong or problem while downloading the data -  for SQL download.{e}")
        return abort(400, "Please verify SQL query \n problem while downloading the data")

if __name__ == '__main__':
    app.run(debug= True)