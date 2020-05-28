import sys 
sys.path.append('..') 
from config import * 

# load credentials from Airflow metastore
from airflow.hooks.base_hook import BaseHook

"""hook
config = BaseHook.get_connection("conn_id") 
config_key = config.login
config_secret = config.password
"""


def create_db(host:str, db:str, user: str, pswd: str, table: str)-> None:
    engine = create_engine('mysql+mysqldb://%s:%s@localhost/%s'%(user,pswd, db))

    if not database_exists(engine.url):
        create_database(engine.url)
        try:
            conn_id =MySQLdb.connect(host=host,database=db, user= user, passwd = pswd)
            logger.info('successfully connect to db')
            curr= conn_id.cursor() 
            create_table="""
        create table if not exists %s(
            city TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            humudity REAL, 
            pressure REAL,
            min_temp REAL, 
            max_temp REAL,
            temp REAL,
            weather TEXT
        )
        """ %table
            curr.execute(create_table)
            logger.info('load Data')  
            conn_id.commit()
            conn_id.close()  

        except:
            logger.warning('failed to connect to db') 

         
create_db(HOST, DB, USER, PSWD, TABLE)