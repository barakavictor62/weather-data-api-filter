#!python

#code for filtering json data from ibm weather company data API and pushing data to database

import json
import MySQLdb

#establishing database connection
db = MySQLdb.connect(user='root', password='',host='127.0.0.1',database='alligator_db')

cursor = db.cursor()

try:
    #filter criteria definition
    parameters= ['fcst_valid','long_daypart_name', 'temp','pop_phrase', 'phrase_12char', 'uv_index']

    #reading from downloaded json data
    with open("turkana.json") as location_data:
        data_summary = json.load(location_data)

        #filtering data based on filter criteria and inserting to database
        for x in range(0,3):
            filtered = data_summary['forecasts'][x]['day']
            parameter_values = {}
            for key in parameters:
                parameter_values[key]=str(filtered[key])
            formatted = parameter_values
            sql ="INSERT INTO WAJIR(fcst_valid, long_daypart_name,temp, pop_phrase, phrase_12char, uv_index)\
            VALUES ('%s', '%s', '%d', '%s', '%s','%d' )" % \
            (formatted['fcst_valid'], formatted['long_daypart_name'],int(formatted['temp']), formatted['pop_phrase'],formatted['phrase_12char'],int(formatted['uv_index']))
            cursor.execute(sql)
            db.commit()
except:
   # Rollback in case there is any error
    db.rollback()
db.close()
