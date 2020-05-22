from config import * 

#use yml file to store all the db sys login info 
conf = Configuration("config/default.yml")
host,user,passwd,db = conf.getMySQLmetadata()
kafka_host,kafka_port = conf.getBrokermetadata()
topic,consumergroup = conf.getConsumermetadata()
schemaAvro = conf.getAvroSchema()  
broker_config=kafka_host+":"+str(kafka_port)

def run():
    consumer = KafkaConsumer(bootstrap_servers=[broker_config], 
                                auto_offset_reset='earliest',
                                enable_auto_commit= True,
                                group_id=consumergroup) 
                                # value_deserializer=lambda x: loads(x.decode('utf-8')) 

    consumer.subscribe(['kafkatesting'])
    for message in consumer:
        print(str(message)) 
        # data=json.loads(message.value)  
        # print(data) 

        conn = MySQLdb.connect(host,user,passwd,db)
        cursor =conn.cursor() 
        #for message is bytes format, need to use avro to parse dataset, TODO 
        cursor.execute("INSERT INTO whatever (stations) values (%s)"%(message.offset))
        # cursor.execute("INSERT INTO whatever (test) values (%s)"%(data)) 
        
        conn.commit() 
        logging.info('Use kafka as the sink to export data to DWH, this case is mysql')
    conn.close() 

# schema = avro.schema.parse(open(schemaAvro).read())
        # bytes_reader = io.BytesIO(message.value)
        # decoder = avro.io.BinaryDecoder(bytes_reader)
        # # reader = avro.io.DatumReader(schema)
        # # user1 = reader.read(decoder)
        # insertIntoDatabase(bytes_reader) 
if __name__=='__main__':
    run()