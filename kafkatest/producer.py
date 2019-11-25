from config import * 

def create_producer(): 
    producer = KafkaProducer(bootstrap_servers=['localhost:9092','localhost:9093'],
    #ingest json 
    value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    with open('data/2019-11-25.json', 'r') as inputfile:
        data=json.load(inputfile) 
    values= list() 
    for k,v in data.items(): 
            # producer.send('topictest', k) 
            # values.append(v)
            producer.send('kafkadev', v) 
            # producer.send('topictest',b'test %d run' %v) 
            print('Use Kafka as source to get API data is OK!') 
            sleep(1)  

if __name__=='__main__':
    create_producer()  
