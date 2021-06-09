from typing import Collection
from flask import Flask, request, jsonify,abort, after_this_request
import tempfile
import os
import subprocess
import json
from flask.ctx import after_this_request
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from connection.db_connect_nosql import *
from config.logger import *

logger = logging.getLogger('cassandra')
setup_logger(logger,'logs/cassandra/cassandra.logs')

A=Cnxn
casndra_sessn=A.cassandra_conn()

app = Flask(__name__)

@app.route('/cassandra/create', methods = ['POST'])
def cassandra_create():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute(query['qry']).one()
        logger.info("Table created for cassandra create")
        return "Table created"
    except Exception as e:
        logger.error("There is something wrong in query or given namespace is already available for cassandra create.{e}")
        return abort(400, "There is something wrong in query or given namespace is already available")

@app.route('/cassandra/select', methods = ['POST'])
def cassandra_select():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute(query['qry']).all()
        print('rows',rows)
        print('type of rows',type(rows))
        logger.info("Select query perfectly ran for cassandra select")
        return jsonify(rows)
    except Exception as e:
        logger.error("There is something wrong in query for cassandra select.{e}")
        return abort(400, "There is something wrong in query")


@app.route('/cassandra/insert_one', methods = ['POST'])
def cassandra_insert_one():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute(query['qry']).all()
        logger.info("One row inserted for cassandra insert one")
        return "Data Inserted"
    except Exception as e:
        logger.error("There is something wrong in query for cassandra insert one.{e}")
        return abort(400, "There is something wrong in query")

@app.route('/cassandra/insert_multi', methods = ['POST'])
def cassandra_insert():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute("BEGIN BATCH "+ query['qry']+ "APPLY BATCH;").all()
        logger.info("Multiple rows inserted for cassandra insert many")
        return "Data Inserted"
    except Exception as e:
        logger.error("There is something wrong in query for cassandra insert many.{e}")
        return abort(400, "There is something wrong in query")


@app.route('/cassandra/bulk_insert', methods = ['POST'])
def cassandra_bulk_insert():
    csv_data=request.files.get('file')
    if request.files.get('file') == 'file':
        return abort(400, "Key name is different from file")
    if csv_data.filename == '':
        return abort(400, "File not attached in body. Please attach csv document.")
    temp_folder = tempfile.TemporaryDirectory()
    file_name = csv_data.filename
    is_file = True
    file_path = os.path.join(temp_folder.name, file_name)
    csv_data.save(file_path)
    df=pd.read_csv(file_path)
    try:
        for i in range(len(df)):
            casndra_sessn.execute("INSERT INTO mishra_test.bulk_insert (id, emp_name, emp_age, sex) Values ('" +df.iloc[i]['id'] + "', '" + df.iloc[i]['emp_name'] + "', '" + str(df.iloc[i]['emp_age']) + "', '" + df.iloc[i]['sex'] + "')")
            logger.info("CSV data has been inserted for cassandra bulk insert")
        return "All CSV Data Inserted"
    except Exception as e:
        logger.error("There is something wrong in query for cassandra bulk insert.{e}")
        return abort(400, "There is something wrong in query")

@app.route('/cassandra/update', methods = ['POST'])
def cassandra_update():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute(query['qry']).all()
        logger.info("Data has been updated for cassandra update")
        return "rows updated"
    except Exception as e:
        logger.error("There is something wrong in query for cassandra update.{e}")
        return abort(400, "There is something wrong in query")

@app.route('/cassandra/delete', methods = ['POST'])
def cassandra_delete():
    query = request.get_json(['qry'])
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as qry.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    try:
        rows = casndra_sessn.execute(query['qry']).all()
        logger.info("Data has been deleted for cassandra delete")
        return "rows deleted"
    except Exception as e:
        logger.error("There is something wrong in query for cassandra delete.{e}")
        return abort(400, "There is something wrong in query")

if __name__ == '__main__':
    app.run(debug= True)