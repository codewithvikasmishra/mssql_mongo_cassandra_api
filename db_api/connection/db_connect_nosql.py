import collections
import pymongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from .dbconfig import *
from config.logger import *

logger = logging.getLogger('dbconfig')
setup_logger(logger,'logs/nosql_dbconfig.logs')

class Cnxn():

    def mongodb_conn():
        A=Read_Config()
        db_config = A.read_config('mongodb')
        try:
            cloud_client = pymongo.MongoClient("mongodb+srv://"+db_config['id']+":"+db_config['pwd']+"@cluster0.uebov.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            db = cloud_client[db_config['name_space']]
            collection=db[db_config['database']]
            logger.info("Connection established for mongo database")
            return cloud_client,db,collection
        except Exception as e:
            logger.error(f"Error while connecting the mongo database.{e}")
            return "Error while connecting the mongo database"

    def cassandra_conn():
        A=Read_Config()
        db_config = A.read_config('cassandra')
        try:
            cloud_config= {
                'secure_connect_bundle': db_config['secure_connect']
                }
            auth_provider = PlainTextAuthProvider(db_config['id'], db_config['pwd'])
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            logger.info("Connection established for cassandra database")
            return session
        except Exception as e:
            logger.error(f"Error while connecting the cassandra database.{e}")
            return "Error while connecting the cassandra database"