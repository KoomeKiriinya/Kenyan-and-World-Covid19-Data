from flask import Flask,Response,jsonify,flash
import json
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
app=Flask(__name__)
@app.route('/')
def index():
     return_data='Welcome to the Flask Data API homepage! </br></br> find link here to Kenyan Data:</br></br> <a href="\KenyanData">Kenyan Data</a> </br></br> find link here to World Data:</br></br><a href="\WorldData">World Data</a> '
     return return_data
@app.route('/KenyanData')
def get_kenyan_data():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    cred=ServiceAccountCredentials.from_json_keyfile_name(r'/Users/user/Downloads/Kenyan Data/APIgeocode-bf744162b9ca.json')
    client=gspread.authorize(cred)
    sheet=client.open("Covid 19 Data").sheet1
    Date=list(map(lambda x:x['Date'],sheet.get_all_records()))
    County=list(map(lambda x:x['County'],sheet.get_all_records()))
    Number=list(map(lambda x:x['Number'],sheet.get_all_records()))
    #remember to replace data as is the shapefile check on PowerBI.
    data=pd.DataFrame(columns=['Date','County','Number','Cummulative Sum'])
    data['Date']=Date
    data['County']=County
    data['Number']=Number
    data['Cummulative Sum']=data.groupby('County')['Number'].cumsum()
    return Response(data.to_json(orient='records'),mimetype='application/json')      
@app.route('/WorldData')
def get_world_data():
    #urls from the required sources
    url_confirmed_cases="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    url_death_cases="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
    url_recovered_cases="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
    #read the required url's
    time_series_data_confirmed=pd.read_csv(url_confirmed_cases, error_bad_lines=False)
    time_series_data_death_cases=pd.read_csv(url_death_cases, error_bad_lines=False)
    time_series_data_recovered_cases=pd.read_csv(url_recovered_cases, error_bad_lines=False)
    #get the idvars and value vars of the data
    date_columns=time_series_data_confirmed.iloc[:,4:].columns
    id_vars=time_series_data_confirmed[['Province/State','Country/Region','Lat','Long']].columns
    #melt the df's accordingly
    all_cases=pd.melt(time_series_data_confirmed,id_vars=id_vars,value_vars=date_columns,var_name='Date',value_name='Confirmed Cases Values')
    death_cases=pd.melt(time_series_data_death_cases,id_vars=id_vars,value_vars=date_columns,var_name='Date',value_name='Death Cases Values')
    recovered_cases=pd.melt(time_series_data_recovered_cases,id_vars=id_vars,value_vars=date_columns,var_name='Date',value_name='Recovered Cases Values')
    #convert the date column to date
    all_cases['Date Value']=pd.to_datetime(all_cases['Date'])
    death_cases['Date Value']=pd.to_datetime(death_cases['Date'])
    recovered_cases['Date Value']=pd.to_datetime(recovered_cases['Date'])
    #have a unique identifier for each record
    all_cases['Identifier']=all_cases['Country/Region']+"-"+all_cases['Date']
    death_cases['Identifier']=death_cases['Country/Region']+"-"+death_cases['Date']
    recovered_cases['Identifier']=recovered_cases['Country/Region']+"-"+recovered_cases['Date']
    return_data={'Confirmed Cases':all_cases.to_json(orient='records'),
    'Death Cases':death_cases.to_json(orient='records'),
    'Recovered Cases':recovered_cases.to_json(orient='records')}
    return jsonify(return_data)                  
if __name__=='__main__':
    app.run(debug=True)
