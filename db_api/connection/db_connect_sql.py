import pyodbc
import collections
from .dbconfig import *
from config.logger import *

logger = logging.getLogger('dbconfig')
setup_logger(logger,'logs/sql_dbconfig.logs')

class Cnxn():

    def sql_conn():
        A=Read_Config()
        db_config = A.read_config('MSSQL')
        conn = None
        try:
            conn = pyodbc.connect(**db_config)
            cursor=conn.cursor()
            logger.info("Cursor prepared for mssql database")
            try:
                sql_command="""SELECT @@Version"""
                cursor.execute(sql_command)
                for row in cursor:
                    logger.info(f'SQL version = {row}')
            except Exception as e:
                logger.error("Table not available. {e}")
        except Exception as e:
            logger.error("mssql error.{e}")
        return conn