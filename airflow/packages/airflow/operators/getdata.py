from config import * 


def get_weather():
    params={'q':'Shanghai, CN', 'appid':API_KEY}

    result = requests.get('http://api.openweathermap.org/data/2.5/weather?', params)

    if result.status_code == 200:
        logger.info('Connection is OK')
        json_data = result.json() 
        fileName = str(dt.now().date())+'.json' 
        tot = os.path.join(os.getcwd(), fileName)
        
        time.sleep(1)


        with open(tot, 'w') as outputfile:
            json.dump(json_data, outputfile)

    else:
        logger.warn('Failed to connect API for data fetching')
