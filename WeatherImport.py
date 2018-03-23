import requests
import json
import sys
import os
import pyodbc
import datetime

#credentials file for SQL login and api key
sys.path.append(os.path.join('..'))
import credentials


owm_api_key = credentials.OWM_API['key']

#owm city id
city_id = '4335045'

url = 'http://api.openweathermap.org/data/2.5/weather?id='+city_id+'&units=imperial&APPID='+owm_api_key

r = requests.post(url,'')

#load the json data returned from the get
json_data = json.loads(r.text)

formatted_data = {}

#getting data into a flat dict to insert into sql
for entry in json_data:
	#pushing into json format makes the data do weird things - some are dicts and some are lists with a dict as it's only entry
	if isinstance(json_data[entry],(dict)):
		for subentry in json_data[entry]:
			formatted_data.update({str(entry)+(str(subentry)[0]).upper()+str(subentry)[1:]:json_data[entry][subentry]})
	elif isinstance(json_data[entry],(list)):
		#lists returned from json convert are just 1 dict entry
		for subentry in json_data[entry][0]:
			formatted_data.update({str(entry)+(str(subentry)[0]).upper()+str(subentry)[1:]:json_data[entry][0][subentry]})
	else:
		formatted_data.update({str(entry):json_data[entry]})

formatted_data['dt'] = datetime.datetime.fromtimestamp(formatted_data['dt'])
formatted_data['sysSunrise'] = datetime.datetime.fromtimestamp(formatted_data['sysSunrise'])
formatted_data['sysSunset'] = datetime.datetime.fromtimestamp(formatted_data['sysSunset'])

data_to_insert = {}

fields_to_keep = ['weatherId','weatherMain','weatherDescription','mainTemp','mainPressure','mainHumidity','mainTemp_min','mainTemp_max','visibility','windSpeed','cloudsAll','rain3h','snow3h','dt','sysSunrise','sysSunset','id','name']

for entry in fields_to_keep:
	#if the field is one we want to keep and is in formatted_data then we want to insert it into the sql db
	if entry in formatted_data:
		data_to_insert.update({entry:formatted_data[entry]})
	else:
		data_to_insert.update({entry:""})
		
#renaming key values
data_to_insert['cityID'] = data_to_insert.pop('id')
data_to_insert['sunrise'] = data_to_insert.pop('sysSunrise')
data_to_insert['sunset'] = data_to_insert.pop('sysSunset')
data_to_insert['dateUTC'] = data_to_insert.pop('dt')
data_to_insert['temp'] = data_to_insert.pop('mainTemp')
data_to_insert['pressure'] = data_to_insert.pop('mainPressure')
data_to_insert['humidity'] = data_to_insert.pop('mainHumidity')
data_to_insert['maxTemp'] = data_to_insert.pop('mainTemp_max')
data_to_insert['minTemp'] = data_to_insert.pop('mainTemp_min')
data_to_insert['cityName'] = data_to_insert.pop('name')


driver= '{SQL Server}'
server = credentials.BW_SQL['server']
database = credentials.BW_SQL['database']
username = credentials.BW_SQL['username']
password = credentials.BW_SQL['password']
cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+password)
cnxn.autocommit = True
cur = cnxn.cursor()
		
columns = ', '.join(data_to_insert.keys())
values = []
for entry in data_to_insert.values():
	if(isinstance(entry,float)):
		values.append(str(entry))
	elif(str(entry).isnumeric()):
		values.append(str(entry))
	else:
		values.append("'"+str(entry)+"'")
	
placeholders = ', '.join(values)
sql = 'INSERT INTO [dbo].[Weather] ({}) VALUES ({})'.format(columns, placeholders)

print(sql)

cur.execute(sql)
cur.close()
cnxn.close()
