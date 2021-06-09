from typing import Collection
from flask import Flask, request, jsonify,abort, after_this_request
import tempfile
import os
import subprocess
import json
from flask.ctx import after_this_request
import pandas as pd
from connection.db_connect_nosql import *
from config.logger import *

logger = logging.getLogger('mongodb')
setup_logger(logger,'logs/mongodb/mongodb.logs')

A=Cnxn
mongo_cnxn=A.mongodb_conn()

app = Flask(__name__)

@app.route('/mongo/create', methods = ['POST'])
def mongo_create():
    query = request.get_json(['cllct_nme'])
    if request.get_json().keys()!={'cllct_nme'}:
        return abort(400, "Please pass the key as cllct_nme.")
    elif request.get_json('cllct_nme')['cllct_nme']=='':
        return abort(400, "Please pass key and value.")
    try:
        mongo_cnxn[1][query['cllct_nme']]
        logger.info("Collection created for mongo create")
        return (query['cllct_nme']+" collection has been created")
    except Exception as e:
        logging.error(f"Collection with same name is already available for mongo create.,{e}")
        return abort(400,"Collection with same name is already available")

@app.route('/mongo/select', methods = ['POST'])
def mongo_select():
    if request.get_json().keys()!={'qry'}:
        return abort(400, "Please pass the key as cllct_nme.")
    elif request.get_json('qry')['qry'].keys()=={''}:
        return abort(400, "Please pass keys.")
    elif request.get_json('qry')['qry'].get('_id')=='':
        return abort(400, "Please pass value.")
    elif request.get_json('qry')['qry']=='':
        return abort(400, "Please pass key and value.")
    query = request.get_json(['qry'])
    try:
        res=[]
        result=mongo_cnxn[2].find(query['qry'])
        try:
            for data in result:
                res.append(data)
                logger.info("select query ran for mongo select")
                return jsonify(res)
        except Exception as e:
            logging.error(f"filter condition is not valid for mongo select.{e}")
            return abort(400,"Given filter condition is not valid")
    except Exception as e:
        logging.error(f"Something wrong in query for mongo select.{e}")
        return abort(400,"Something wrong in query")


@app.route('/mongo/insert_one', methods = ['POST'])
def mongo_insert_one():
    print(list(request.get_json().keys()))
    if list(request.get_json().keys()).sort()!=['qry', 'collection_name'].sort():
        return abort(400, "Please pass valid keys")
    elif list(request.get_json().keys())==['qry',''] or list(request.get_json().keys())==['','collection_name'] or list(request.get_json().keys())==['','qry'] or list(request.get_json().keys())==['collection_name',''] or list(request.get_json().keys())==['','']:
        return abort(400, "Please pass keys.")
    elif request.get_json('collection_name')['collection_name']=='' or request.get_json('qry')['qry'].get('_id')=='' or request.get_json('qry')['qry'].get('name')=='' or request.get_json('qry')['qry'].get('address')=='':
        return abort(400, "Please pass value.")
    elif request.get_json('qry')['qry']=='' or request.get_json('collection_name')['collection_name']=='':
        return abort(400, "Please pass key and value.")

    cllctn = request.get_json(['collection_name'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    try:
        query = request.get_json(['qry'])
        new_cllc.insert_one(query['qry'])
        logger.info("Single record inserted for mongo insert one")
        return ("Data Inserted in collection ")
    except Exception as e:
        logger.error(f"Either id is not unique or someting wrong in query for mongo insert one.{e}")
        return abort(400,"Either id is not unique or someting wrong in query")
    

@app.route('/mongo/insert_many', methods = ['POST'])
def mongo_insert_many():
    print(request.get_json('qry')['qry'][0].get('name'))
    if list(request.get_json().keys()).sort()!=['qry', 'collection_name'].sort():
        return abort(400, "Please pass valid keys")
    elif list(request.get_json().keys())==['qry',''] or list(request.get_json().keys())==['','collection_name'] or list(request.get_json().keys())==['','qry'] or list(request.get_json().keys())==['collection_name',''] or list(request.get_json().keys())==['','']:
        return abort(400, "Please pass keys.")
    elif request.get_json('collection_name')['collection_name']=='':
        return abort(400, "Please pass value for collection_name.")
    elif request.get_json('qry')['qry']=='' or request.get_json('collection_name')['collection_name']=='':
        return abort(400, "Please pass key and value.")
    else:
        for i in request.get_json('qry')['qry']:
            if i['_id']=='' or i['name']=='' or i['address']=='':
                return abort(400, "Please pass value.")

    cllctn = request.get_json(['collection_name'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    try:
        query = request.get_json(['qry'])
        new_cllc.insert_many(query['qry'])
        logger.info("Multiples record inserted for mongo insert many")
        return ("Data Inserted")
    except Exception as e:
        logger.error("Either id is not unique or someting wrong in query for mongo insert many.{e}")
        return abort(400, "Either id is not unique or someting wrong in query")

@app.route('/mongo/bulk_insert', methods = ['POST'])
def mongo_bulk_insert():
    csv_data=request.files.get('file')
    cllctn_nm=request.form.get('collection_name')
    if csv_data.filename == '':
        return abort(400, "File not attached in body. Please attach csv document.")
    elif cllctn_nm=='':
        return abort(400, "Please fill the text for collection name.")
    temp_folder = tempfile.TemporaryDirectory()
    file_name = csv_data.filename
    is_file = True
    file_path = os.path.join(temp_folder.name, file_name)
    csv_data.save(file_path)
    data=pd.read_csv(file_path)
    payload = json.loads(data.to_json(orient='records'))

    try:
        new_cllc=mongo_cnxn[1][cllctn_nm]
        new_cllc.insert(payload)
        logger.info(f"CSV data has been inserted for mongo bulk insert from {file_name}")
        return ("Data inserted from "+file_name)
    except Exception as e:
        logger.error("There is something wrong in csv file for mongo bulk insert {e}")
        return abort(400, "There is something wrong in csv file")

    
    @after_this_request
    def cleanup(folder):
        temp_folder.cleanup()
        return folder

@app.route('/mongo/update_one', methods = ['POST'])
def mongo_update_one():
    cllctn = request.get_json(['collection_name'])
    query = request.get_json(['qry'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    old_qry = query['qry'][0]['old_qry']
    new_qry = query['qry'][1]['new_qry']
    new_cllc.update_one(old_qry,new_qry)
    logger.info("One records has been updated for mongo update_one")
    return ("one record updated")

@app.route('/mongo/update_many', methods = ['POST'])
def mongo_update_many():
    cllctn = request.get_json(['collection_name'])
    query = request.get_json(['qry'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    old_qry = query['qry'][0]['old_qry']
    new_qry = query['qry'][1]['new_qry']
    new_cllc.update_many(old_qry,new_qry)
    logger.info("Multiples records has been updated for mongo update_many")
    return ("All relevant record's updated")

@app.route('/mongo/delete_one', methods = ['POST'])
def mongo_delete_one():
    cllctn = request.get_json(['collection_name'])
    query = request.get_json(['qry'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    new_cllc.delete_one(query['qry'])
    logger.info("One record has been deleted for mongo delete one")
    return ("one record deleted")

@app.route('/mongo/delete_many', methods = ['POST'])
def mongo_delete_many():
    cllctn = request.get_json(['collection_name'])
    query = request.get_json(['qry'])
    new_cllc=mongo_cnxn[1][cllctn['collection_name']]
    new_cllc.delete_many(query['qry'])
    logger.info("Multiples record has been deleted for mongo delete many")
    return ("All record's deleted with given criteria")

@app.route('/mongo/download', methods = ['POST'])
def mongo_bulk_download():
    db = request.get_json(['db'])
    cllctn = request.get_json(['collection'])
    print(db['db'],cllctn['collection'])
    qry = "mongoexport --db " + db['db'] + " --collection "+ cllctn['collection'] +" --type=csv"+ " --out="+ "'/home/vikesh/Documents/ineuron/db_api/csv/download_API/mongo_collection.csv'"
    print(qry)
    subprocess.call(qry)
    return "let's try"

if __name__ == '__main__':
    app.run(debug= True)