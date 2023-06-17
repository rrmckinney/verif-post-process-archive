import pandas as pd
import datetime 
from datetime import timedelta
import numpy as np

start_date = '230530'
end_date = '230606'

def listofdates(obs = False):
    if obs == False:
        start = datetime.datetime.strptime(start_date, "%y%m%d").date()
        end = datetime.datetime.strptime(end_date, "%y%m%d").date()

    elif obs == True:
        startday = 0 #forhour 1
        endday = 7 #for hour 180
        
        start = datetime.datetime.strptime(start_date, "%y%m%d").date() + timedelta(days=startday)
        end = datetime.datetime.strptime(end_date, "%y%m%d").date() + timedelta(days=endday)
    
    numdays = (end-start).days 
    date_list = [(start + datetime.timedelta(days=x)).strftime("%y%m%d") for x in range(numdays+1)]
    return(date_list)
def get_filehours(hour1,hour2):
    
    hours_list = []
    for i in range(hour1,hour2+1):
        i = i - 1
        if i < 10:
            hour = "0" + str(i)

        else:
            hour = str(i)
        
        hours_list.append(hour)
        
    return(hours_list)

date_list_obs = listofdates(obs=True)
df_new = pd.DataFrame()

for day in date_list_obs:
    
    dates = [day] * 24
    filehours_obs = get_filehours(1,24)

    #dates = pd.to_datetime(date_list_obs+ ' ' + filehours_obs, format='%y%m%d %H')
    df = pd.DataFrame({'date':dates, 'time': filehours_obs})
    print(df)
    df['datetime'] = pd.to_datetime(df['date'] + ' ' +df['time'],format = '%y%m%d %H')

    df_new = pd.concat([df_new, df])

df_new['obs'] = np.nan
df_new['fcst'] = np.nan

print(df_new['obs'][:60])