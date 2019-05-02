# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 10:39:36 2019

@author: amarnath.mitra
"""

import io
import requests
import pandas as pd
import datetime as dt
import calendar

# collection: https://gcbeta.marginalunit.com/reflow/collections
# cases == miso: https://gcbeta.marginalunit.com/reflow/miso-se/cases
# pnodes == test case: https://gcbeta.marginalunit.com/reflow/miso-se/miso_se_20190412-1800_AREVA/pnodes

# REFLOW Username & Password
reflow_auth = ('amarnath.mitra', '5vgpc36c')

def get_df(url, auth):
    resp = requests.get(url, auth = auth)
    if resp.status_code != 200:
        print(resp.text)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))

# ***** query new pnode data for a given iso *****
last_case = len(df_cases)

df_collection = get_df('https://gcbeta.marginalunit.com/reflow/collections', reflow_auth)
df_collection.head()

# collection == miso-se
new_cases = get_df('https://gcbeta.marginalunit.com/reflow/miso-se/cases', reflow_auth)
new_cases.head()
new_cases.tail()

latest_case = len(new_cases)

if latest_case == last_case:
    print("PNode Data is Up-to-Date")
else:
    for i in range(last_case, latest_case+1):
        
        print("% Completed: " + str(round((100*i/len(df_cases)), 2)) + "%")
        
        case = new_cases.code[i]
        url = f'https://gcbeta.marginalunit.com/reflow/miso-se/{case}/pnodes'
        df_pnodes = get_df(url, reflow_auth)
        
        tc = case
        ts = tc.split('_')[2]
        date = ts.split('-')[0]
        hour = ts.split('-')[1][:2]
        d = dt.datetime.strptime(date, '%Y%m%d').date()
        year = d.year
        month = d.month
        day = d.day
        weekday = calendar.day_name[d.weekday()]
        
        df_pnodes['iso'] = 'miso-se'
        df_pnodes['case'] = case
        df_pnodes['year'] = year
        df_pnodes['month'] = month
        df_pnodes['day'] = day
        df_pnodes['hour'] = hour
        df_pnodes['weekday'] = weekday
        df_pnodes['peak_time'] = 'peak_time' # to be determined later
        
        df_pnodes_output = df_pnodes[['iso', 'case', 'year', 'month', 'day', 'hour', 'pnode_name', 'bus_id', 'weight', 'weekday', 'peak_time']]
        
        file_name = f'{case}_pnodes'
        path = f'C://Users//amarnath.mitra//Desktop//miso-se_pnode_data//{file_name}.csv'
        df_pnodes_output.to_csv(path, index = True)

df_cases = new_cases  

# ***** end of task == new query is appended in the existing database *****

# ===========================================================================
# ===========================================================================

# ***** build the pnode database for a given iso *****
    
# first: we query the collection
df_collection = get_df('https://gcbeta.marginalunit.com/reflow/collections', reflow_auth)
df_collection.head()

# second: we query the cases for a given iso
# collection == miso-se
df_cases = get_df('https://gcbeta.marginalunit.com/reflow/miso-se/cases', reflow_auth)
df_cases.head()
df_cases.tail()
len(df_cases)

# third: download case-wise pnode data for the given iso == till date
for i in range(len(df_cases)):
#for i in range(1, 3):    # < used for testing >
    print("% Completed: " + str(round((100*i/len(df_cases)), 2)) + "%")
    
    case = df_cases.code[i]
    url = f'https://gcbeta.marginalunit.com/reflow/miso-se/{case}/pnodes'
    df_pnodes = get_df(url, reflow_auth)
    
    tc = case
    ts = tc.split('_')[2]
    date = ts.split('-')[0]
    hour = ts.split('-')[1][:2]
    d = dt.datetime.strptime(date, '%Y%m%d').date()
    year = d.year
    month = d.month
    day = d.day
    weekday = calendar.day_name[d.weekday()]

    df_pnodes['iso'] = 'miso-se'
    df_pnodes['case'] = case
    df_pnodes['year'] = year
    df_pnodes['month'] = month
    df_pnodes['day'] = day
    df_pnodes['hour'] = hour
    df_pnodes['weekday'] = weekday
    df_pnodes['peak_time'] = 'peak_time'
    
    df_pnodes_output = df_pnodes[['iso', 'case', 'year', 'month', 'day', 'hour', 'pnode_name', 'bus_id', 'weight', 'weekday', 'peak_time']]
    
    file_name = f'{case}_pnodes' 
    path = f'C://Users//amarnath.mitra//Desktop//miso-se_pnode_data//{file_name}.csv'
    df_pnodes_output.to_csv(path, index = True)
    
# ***** end of task == building of pnode database for a given iso ***** 
 




