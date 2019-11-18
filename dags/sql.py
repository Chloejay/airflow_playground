from config import * 

def create_db():
    host='localhost'
    db='airflow_test'
    user='root'
    passwd='password'
    table='city_weather'

    engine= create_engine('mysql+mysqldb://%s:%s@localhost/%s'%(user,passwd, db))

    if not database_exists(engine.url):
        create_database(engine.url)
    try:
        conn=MySQLdb.connect(host=host,database=db, user= user, passwd= passwd)
        logging.info('successfully connect to db')
        curr= conn.cursor() 
    else: 
        logging.warning('failed to connect to db') 
    
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
    conn.commit()
    conn.close() 

if __name__=='__main__':
    create_db() 

