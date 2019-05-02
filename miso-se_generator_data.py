# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 12:34:51 2019

@author: amarnath.mitra
"""

import io
import os
import requests
import pandas as pd
import datetime as dt
import calendar

# collection: https://gcbeta.marginalunit.com/reflow/collections
# cases == miso: https://gcbeta.marginalunit.com/reflow/miso-se/cases
# generator == test case: https://gcbeta.marginalunit.com/reflow/miso-se/miso_se_20190416-1800_AREVA/generators
# generator data == test generator: https://gcbeta.marginalunit.com/reflow/miso-se/generator?name=08BROWN%20%20BROWNLF

# REFLOW Username & Password
reflow_auth = ('amarnath.mitra', '5vgpc36c')

def get_df(url, auth):
    resp = requests.get(url, auth = auth)
    if resp.status_code != 200:
        print(resp.text)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

# part 1
# ***** build the generator database for a given iso *****

dir0 = 'C://Users//amarnath.mitra//Desktop'
os.chdir(dir0)
    
# first: we query the collection
df_collection = get_df('https://gcbeta.marginalunit.com/reflow/collections', reflow_auth)
df_collection.head()

# second: we query the cases for a given iso
# collection == miso-se
df_cases = get_df('https://gcbeta.marginalunit.com/reflow/miso-se/cases', reflow_auth)
df_cases.head()
df_cases.tail()
lc = len(df_cases)

# third: dowload case-wise generators and create an unique generator list for an iso
df_gen = pd.DataFrame() # empty dataframe
for i in range(2):
#for i in range(lc):
    print("Generator List % Completed: " + str(round((100*i/lc), 2)) + "%")
    case = df_cases.code[i]
    url = f'https://gcbeta.marginalunit.com/reflow/miso-se/{case}/generators'
    df_gens = get_df(url, reflow_auth)
    dfi = df_gens[['name','bus_kv', 'area_name', 'zone_name']]
    df_gen = df_gen.append(dfi, ignore_index=True)
    
df_gen.drop_duplicates(subset='name', inplace=True)
df_generators = df_gen

# fourth: download generator-wise data
lg = len(df_generators)
for j in range(2):
#for j in range(lg):
    print("Generator Temp Data % Completed: " + str(round((100*j/lg), 2)) + "%")
    generator = df_generators.name[j]
    url_gd = f'https://gcbeta.marginalunit.com/reflow/miso-se/generator?name={generator}'
    df_gen_data = get_df(url_gd, reflow_auth)
    
    df_gen_data['iso'] = 'miso-se'
    df_gen_data['bus_kv'] = df_generators.bus_kv[j]
    df_gen_data['area_name'] = df_generators.area_name[j]
    df_gen_data['zone_name'] = df_generators.zone_name[j]
    df_gen_data['peak_time'] = 'peak_time' # to be determined later   
    df_gen_data['hour'] = 'hour'
    df_gen_data['year'] = 'year'
    df_gen_data['month'] = 'month'
    df_gen_data['day'] = 'day'
    df_gen_data['weekday'] = 'weekday'
    
    df_gen_data_output = df_gen_data[['iso', 'case', 'year', 'month', 'day', 'hour', 'number', 'bus_id', 'name', 'unit_id', 'bus_kv', 'area_name', 'zone_name', 'pg', 'pmax', 'pmin', 'pq', 'qmax', 'qmin', 'v_magnitude', 'base_mva', 'status', 'memo', 'weekday', 'peak_time']]
    
    file_name = f'miso-se_{generator}_gen-data-temp'
    path = f'C://Users//amarnath.mitra//Desktop//miso-se_gen_data_temp//{file_name}.csv'
    df_gen_data_output.to_csv(path, index = False)

# fifth: arrange the generator data
dir = 'C://Users//amarnath.mitra//Desktop//miso-se_gen_data_temp'
os.chdir(dir)
for file in os.listdir(dir):
    if file.endswith('.csv'):
        f = open(file)
        gd = pd.read_csv(f)
        
        lgd=len(gd)
        #for k in range(lgd):
        for k in range(2):
            print("Generator Final Data % Completed: " + str(round((100*k/lgd), 2)) + "%")
            gen = gd.name[k]
            tc = gd.case[k]
            ts = tc.split('_')[2]
            date = ts.split('-')[0]
            hour = ts.split('-')[1][:2]
            gd.hour[k] = hour
            d = dt.datetime.strptime(date, '%Y%m%d').date()
            year = d.year
            gd.year[k] = year
            month = d.month
            gd.month[k] = month
            day = d.day
            gd.day[k] = day
            weekday = calendar.day_name[d.weekday()]
            gd.weekday[k] = weekday
                     
        file_name = f'miso-se_{gen}_gen-data'
        path = f'C://Users//amarnath.mitra//Desktop//miso-se_gen_data_final//{file_name}.csv'
        gd.to_csv(path, index = False)
            
        continue
    else:
        continue

os.chdir(dir0)     
# ***** end of task == building of generator database for a given iso *****    


        
        
        