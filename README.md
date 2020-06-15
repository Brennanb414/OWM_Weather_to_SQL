# OWM_Weather_Import
#################

Very simple tool used to scrape current weather conditions from the Openweathermap API.

This tool queries the Openweathermap API using requests and uses pyodbc to place the data into a SQL Server table that has already been defined.

It requires a "credentials.py" file (not included in this repo) that should be in the directory above the "WeatherImport.py" file and formatted as follows:
```
OWM = {'key': '<your api key here>'}
SQL = {
  'server': '<your server uri here>',
  'database': '<your db name here>',
  'username': '<your username here>',
  'password': '<your pw here>'
}
```

# Room for improvements:
-Query the table we are inserting to to get the current fields it has, then only look for those fields in the flattened record received from OWM. This would replace the "fields_to_keep" list.
-Under the "renaming key values" section the API names vs SQL column names mapping could be done much cleaner
