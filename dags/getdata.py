from config import * 

def get_weather():
    paramaters={'q':'Shanghai, CN', 'appid':API_KEY}

    result= requests.get('http://api.openweathermap.org/data/2.5/weather?', paramaters)

    if result.status_code==200:
        json_data=result.json() 
        fileName= str(dt.now().date())+'.json'
        tot_name=os.path.join(os.path.dirname(__file__),'data', fileName)
        logging.info('Connection is OK') 

        with open (tot_name, 'w') as outputfile:
            json.dump(json_data, outputfile)
    else:
        logging.warn('ERROR for API data fetching') 

if __name__=='__main__':
    get_weather() 

