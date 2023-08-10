#!/usr/bin python

"""
Created in 2023 adapted from code by Eva Gnegy (2021)
@author: Reagan McKinney

These are the functions that feed into the leaderboard-txt-sqlite2.py script that calculates the statistics 
to be plotted on the website. See that file for more info. 
"""
import os
import pandas as pd
import numpy as np
import datetime 
from datetime import timedelta
import sys
import math
import copy
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from scipy import stats
import sqlite3
from utl.funcs import *
import warnings
warnings.filterwarnings("ignore",category=RuntimeWarning)

###########################################################
### -------------------- FILEPATHS ------------------------
###########################################################

#location where obs files are (all sql databases should be in this directory)
obs_filepath = "/verification/Observations/"

#location where forecast files are (immediately within this directory should be model folders, then grid folders, then the sql databases)
fcst_filepath = "/verification/Forecasts/"

#description file for stations
station_file = '/home/verif/verif-post-process/input/station_list_master.txt'

#description file for models
models_file = '/home/verif/verif-post-process/input/model_list.txt'

#folder where the stats save
textfile_folder = '/verification/Statistics/'

#editting mode for textfile

wm = 'a'
###########################################################
### -------------------- INPUTS -- ------------------------
###########################################################

# thresholds for discluding erroneous data 
precip_threshold = 250 #recorded at Buffalo Gap 1961 https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/floods/events-prairie-provinces.html
wind_threshold = 400 #recorded Edmonton, AB 1987 http://wayback.archive-it.org/7084/20170925152846/https://www.ec.gc.ca/meteo-weather/default.asp?lang=En&n=6A4A3AC5-1#tab5
temp_min = -63 #recorded in Snag, YT 1947 http://wayback.archive-it.org/7084/20170925152846/https://www.ec.gc.ca/meteo-weather/default.asp?lang=En&n=6A4A3AC5-1#tab5
temp_max = 49.6 #recorded in Lytton, BC 2021 https://www.canada.ca/en/environment-climate-change/services/top-ten-weather-stories/2021.html#toc2

###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################


# makes a list of the dates you want from start to end, used to make sure the models and obs have the right dates
# obs get their own list because it will be different days than the initializition dates from the models for anything
#   past hours 0-24
def listofdates(start_date, end_date, obs = False):
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

#lists the hour filenames that we are running for
def get_filehours(hour1,hour2):
     
    hours_list = []
    for i in range(hour1,hour2+1):
        i = i-1
        if i < 10:
            hour = "0" + str(i)
        else:
            hour = str(i)
        hours_list.append(hour)
        
    return(hours_list)


def check_variable(variable, station, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24):

    flag = False
    
    if variable == 'SFCTC_KF' or variable == 'SFCTC':
        
        if str(station) in stations_with_SFCTC:
            flag=True
              
    elif variable == 'SFCWSPD_KF' or variable == 'SFCWSPD':
          
        if str(station) in stations_with_SFCWSPD:
            flag=True
            
    elif variable == "PCPTOT":
        
        if str(station) in stations_with_PCPTOT:
            flag=True
            
    elif variable == "PCPT6":
        
        if str(station) in stations_with_PCPTOT or str(station) in stations_with_PCPT6:
            flag=True            
 
    elif variable == "PCPT24":
        
        if str(station) in stations_with_PCPTOT or str(station) in stations_with_PCPT24:
            flag=True        
            
    return(flag)

# checks to see if the right amount of dates exist, which is used for when new models/stations are added
# default station exists for when a new model is added (instead of new station)
def check_dates(start_date, delta, filepath, variable, station):
    flag = True
    
    if len(station) < 4:
        station = "0" +station
    if "PCPT" in variable:
        variable = "PCPTOT"
    
    sql_path = filepath + station + ".sqlite"
    sql_con = sqlite3.connect(sql_path)

    cursor = sql_con.cursor()
    cursor.execute("SELECT DISTINCT Date from 'All'")
    sql_result = cursor.fetchall()
    sql_result = [x[0] for x in sql_result]
    
    if len(sql_result) < delta+1:
        print( "  Not enough dates available for this model/station/variable")
        flag = False
    elif int("20" + start_date) < int(sql_result[0]):
        print("    Model collection started " + str(sql_result[0]) + ", which is after input start_date")
        flag = False
    cursor.close()
    
    return(flag)

def make_df(date_list_obs, start_date, end_date):
    date_list_obs = listofdates(start_date, end_date, obs=True)
    df_new = pd.DataFrame()
    for day in date_list_obs:
        dates = [day] * 24
        filehours_obs = get_filehours(1, 24)
        
        df = pd.DataFrame({'date': dates, 'time': filehours_obs})
        df['datetime'] = pd.to_datetime(df['date']+' '+df['time'], format = '%y%m%d %H')
        
        df_new = pd.concat([df_new, df])
    df_new = df_new.set_index('datetime') 
    return(df_new)

def get_all_obs(delta, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24, all_stations, variable, start_date, end_date, date_list_obs):
    
    print("Reading observational dataframe for " + variable + ".. ")
    
    df_new = make_df(date_list_obs, start_date, end_date)
    
    if variable == 'SFCTC_KF' or variable == 'SFCTC':
        station_list = copy.deepcopy(stations_with_SFCTC)              
    elif variable == 'SFCWSPD_KF' or variable == 'SFCWSPD':  
        station_list = copy.deepcopy(stations_with_SFCWSPD) 
    
    elif variable == "PCPTOT":
        if variable == "PCPT6":
            station_list = [st for st in stations_with_PCPTOT if st not in stations_with_PCPT6 ]
        elif variable == "PCPT24":
            station_list = [st for st in stations_with_PCPTOT if st not in stations_with_PCPT24 ]
        else:
            station_list = copy.deepcopy(stations_with_PCPTOT)        
    
    elif variable == "PCPT6":
        station_list = copy.deepcopy(stations_with_PCPT6) 
    
    elif variable == "PCPT24":
        station_list = copy.deepcopy(stations_with_PCPT24) 
        
    #KF variables are the same as raw for obs
    if "_KF" in variable:
        variable = variable[:-3]
        
    filehours_obs = get_filehours(1,24)
    
    obs_df_60hr = pd.DataFrame()  
    obs_df_84hr = pd.DataFrame()  
    obs_df_120hr = pd.DataFrame() 
    obs_df_180hr = pd.DataFrame() 
    obs_df_day1 = pd.DataFrame()
    obs_df_day2 = pd.DataFrame()
    obs_df_day3 = pd.DataFrame()  
    obs_df_day4 = pd.DataFrame()  
    obs_df_day5 = pd.DataFrame()  
    obs_df_day6 = pd.DataFrame()  
    obs_df_day7 = pd.DataFrame() 
    
    for station in station_list:
        print( "    Now on station " + station) 
         
        if station not in all_stations:
            #print("   Skipping station " + station)
            continue
        if len(station) < 4:
            station = "0" +station
        
        if "PCPT" in variable:
            if check_dates(start_date, delta, fcst_filepath + 'ENS/' + variable + '/fcst.t/', "PCPTOT", station) == False:
                print("   Skipping station " + station + " (not enough dates yet)")
                continue
        else:
            if check_dates(start_date, delta, fcst_filepath + 'ENS/' + variable + '/fcst.t/', variable, station) == False:
                print("   Skipping station " + station + " (not enough dates yet)")
                continue        
        # for hour in filehours_obs:
        #     if float(hour) < 1000:
        #             hour = str(hour).lstrip('0')
        sql_con = sqlite3.connect(obs_filepath + variable + "/" + station + ".sqlite")
        sql_query = "SELECT * from 'All' WHERE date BETWEEN 20" +str(date_list_obs[0]) + " AND 20" + str(date_list_obs[len(date_list_obs)-1])       
        obs = pd.read_sql_query(sql_query, sql_con)
        obs['datetime'] = None
        
        for y in range(len(obs['Time'])):
            hour = int(obs['Time'][y])/100
            obs.loc[y,'datetime'] = pd.to_datetime(obs.loc[y,'Date'], format='%Y%m%d') + timedelta(hours=hour)
        
        obs = obs.set_index('datetime')
        
        df_all = df_new.join(obs, on='datetime')
        
        obs_all = df_all['Val']
        # remove data that falls outside the physical bounds (higher than the verified records for Canada
        for i in range(len(obs_all)):
            
            if variable == 'SFCTC_KF' or variable == 'SFCTC':
                if obs_all[i] > temp_max:
                    obs_all[i] = np.nan
                if obs_all[i] < temp_min:
                    obs_all[i] = np.nan
            
            if variable == 'SFCWSPD_KF' or variable == 'SFCWSPD':
                if obs_all[i] > wind_threshold:
                    obs_all[i] = np.nan
            
            if variable == 'PCPTOT':
                if obs_all[i] > precip_threshold:
                    obs_all[i] = np.nan

        hr60_obs = obs_all[:60]     #84 x 7   (30) 
        hr84_obs = obs_all[:84]     #84 x 7   (30)     
        hr120_obs = obs_all[:120]   #120 x 7  (30) 
        day1_obs = obs_all[:24]     #24 x 7   (30)   
        day2_obs = obs_all[24:48]   #24 x 7   (30)   
        day3_obs = obs_all[48:72]   #24 x 7   (30)     
        day4_obs = obs_all[72:96]   #24 x 7   (30)  
        day5_obs = obs_all[96:120]  #24 x 7   (30)  
        day6_obs = obs_all[120:144] #24 x 7   (30)  
        day7_obs = obs_all[144:168] #24 x 7   (30)  
            
        final_obs_180hr = np.array(obs_all).T
        final_obs_60hr = np.array(hr60_obs).T
        final_obs_84hr = np.array(hr84_obs).T
        final_obs_120hr = np.array(hr120_obs).T
        final_obs_day1 = np.array(day1_obs).T
        final_obs_day2 = np.array(day2_obs).T
        final_obs_day3 = np.array(day3_obs).T
        final_obs_day4 = np.array(day4_obs).T
        final_obs_day5 = np.array(day5_obs).T
        final_obs_day6 = np.array(day6_obs).T
        final_obs_day7 = np.array(day7_obs).T

        obs_df_180hr[station] = final_obs_180hr.flatten() # 1260 (180x7) for each station for weekly
        obs_df_60hr[station] = final_obs_60hr.flatten()
        obs_df_84hr[station] = final_obs_84hr.flatten()   # 588 (84x7)
        obs_df_120hr[station] = final_obs_120hr.flatten() # 840 (120x7)
        obs_df_day1[station] = final_obs_day1.flatten()   # 168 (24x7) 
        obs_df_day2[station] = final_obs_day2.flatten()   # 168 (24x7) 
        obs_df_day3[station] = final_obs_day3.flatten()   # 168 (24x7) 
        obs_df_day4[station] = final_obs_day4.flatten()   # 168 (24x7) 
        obs_df_day5[station] = final_obs_day5.flatten()   # 168 (24x7) 
        obs_df_day6[station] = final_obs_day6.flatten()   # 168 (24x7) 
        obs_df_day7[station] = final_obs_day7.flatten()   # 168 (24x7) 
        
        #extra_point_df[station] = np.array([extra_point])
        # output is a dataframe with the column names as the station, with 420 rows for 60x7 or 60x30
    return(obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7)

# returns the fcst data for the given model/grid
def get_fcst(station, filepath, variable, date_list,filehours, start_date, end_date):
    
    df_new = make_df(date_list, start_date, end_date)

    if "PCPT" in variable:
        variable = "PCPTOT"
    # pulls out a list of the files for the given station+variable+hour wanted   
    sql_con = sqlite3.connect(filepath + station + ".sqlite")
    sql_query = "SELECT * from 'All' WHERE date BETWEEN 20" + str(date_list[0]) + " AND 20" + str(date_list[len(date_list)-1])
    fcst = pd.read_sql_query(sql_query, sql_con)
    
    fcst['datetime'] = None 
    for x in range(len(fcst['Offset'])):
        fcst.loc[x, 'datetime'] = pd.to_datetime(start_date, format='%y%m%d') + timedelta(hours=int(x))
    
    fcst = fcst.set_index('datetime')
    df_all = df_new.join(fcst, on='datetime')
    
    return(df_all['Val'])

# this removes (NaNs) any fcst data where the obs is not recorded, or fcst is -999
def remove_missing_data(fcst, obs):
    for i in range(len(fcst)):        
        if math.isnan(obs[i]) == True:
            fcst[i] = np.nan
            
        if fcst[i] == -999:
            fcst[i] = np.nan
            obs[i] = np.nan
                
    return(fcst,obs) 

def make_textfile(model, grid, input_domain, savetype, date_entry1, date_entry2, time_domain, variable, filepath, MAE, RMSE, corr, len_fcst, numstations):
   
    if "ENS" in model:
        modelpath= model + '/'
    else:
        modelpath = model + '/' + grid + '/'
    f1 = open(textfile_folder + modelpath + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+")       
    read_f1 = np.loadtxt(textfile_folder +  modelpath + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f1 and date_entry2 not in read_f1:
        f1.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f1.write("%3.3f   " % (MAE))
        f1.write(len_fcst + "   ")
        f1.write(numstations + "\n")
    
        f1.close()    
            
    
    f2 = open(textfile_folder +  modelpath + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+")       
    read_f2 = np.loadtxt(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f2 and date_entry2 not in read_f2:
        f2.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f2.write("%3.3f   " % (RMSE))
        f2.write(len_fcst + "   ")
        f2.write(numstations + "\n")
        
        f2.close()  
    
    
    f3 = open(textfile_folder +  modelpath + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+") 
    read_f3 = np.loadtxt(textfile_folder +  modelpath + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f3 and date_entry2 not in read_f3:
        f3.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f3.write("%3.3f   " % (corr))
        f3.write(len_fcst + "   ")
        f3.write(numstations + "\n")
        
        f3.close()  


def trim_fcst(all_fcst,obs_df,station,start,end,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain):

    if variable == "PCPT6":
        if int(end)==int(maxhour):
            trimmed_fcst = all_fcst[start+1:end-5] 
        else:
            trimmed_fcst = all_fcst[start+1:end+1]  
    elif variable == "PCPT24":
        if int(end)==int(maxhour):
            trimmed_fcst = all_fcst[start+1:end-23] 
        else:
            trimmed_fcst = all_fcst[start+1:end+1] 
   
    else:
        trimmed_fcst = all_fcst[start:end]   
       
    fcst_final = np.array(trimmed_fcst).T
    fcst_flat = fcst_final.flatten() 
    
    if variable == "PCPT6":
        fcst_flat = np.reshape(fcst_flat, (-1, 6)).sum(axis=-1) #must be divisible by 6
    if variable == "PCPT24":
        fcst_flat = np.reshape(fcst_flat, (-1, 24)).sum(axis=-1) #must be divisible by 6
        
    obs_flat = np.array(obs_df[station])
    if len(np.shape(obs_flat)) > 1:
        obs_flat = obs_flat[:,1]
    
    ''' was removing entire days from obs so I got rid of it............. 
    f "PCPT" in variable:
        #removes the last point from every day if its at the maxhour, since it doesnt exist for fcst
        if int(end)==int(maxhour): 
            
            if end==180:    
                oneday_length = int((len(obs_flat)+1)/(delta+1))
                obs_flat = np.delete(obs_flat, np.arange(0, obs_flat.size, oneday_length)[1:]-1)
            else:
                oneday_length = int((len(obs_flat))/(delta+1))
                obs_flat = np.delete(obs_flat, np.arange(0, obs_flat.size+oneday_length, oneday_length)[1:]-1)
    '''
    # removes (NaNs) fcst data where there is no obs
    fcst_NaNs,obs_NaNs = remove_missing_data(fcst_flat, obs_flat)  

     
    if input_domain == "small" and variable in ["SFCTC","SFCWSPD"] and all_fcst_KF.any():
        trimmed_fcst_KF = all_fcst_KF[start:end]   
        fcst_final_KF = np.array(trimmed_fcst_KF).T
        fcst_flat_KF = fcst_final_KF.flatten() 
        
        fcst_NaNs,_ = remove_missing_data(fcst_flat, fcst_flat_KF) 
    

    return(fcst_NaNs, obs_NaNs)

def get_statistics(delta, model,grid, input_domain, savetype, date_entry1, date_entry2, maxhour,hour,length,fcst_allstations,obs_allstations,num_stations,totalstations,time_domain,variable,filepath):
    
    if int(maxhour) >= hour:
        fcst_avg = np.nanmean(fcst_allstations,axis=0) 
        obs_avg = np.nanmean(obs_allstations,axis=0)
        fcst_noNaNs, obs_noNaNs = [],[]
        
        for l in range(len(fcst_avg)):
            if np.isnan(fcst_avg[l]) == False:
                fcst_noNaNs.append(fcst_avg[l])
                obs_noNaNs.append(obs_avg[l])
              
        # rounds each forecast and obs to one decimal
        obs_rounded = np.round(obs_noNaNs,1)
        fcst_rounded = np.round(fcst_noNaNs,1)
        
        if len(fcst_rounded) == 0:
            model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,hour,length,totalstations,time_domain,variable,filepath)
        
        else:
            MAE = mean_absolute_error(obs_rounded,fcst_rounded)
            MSE = mean_squared_error(obs_rounded,fcst_rounded)
            RMSE = math.sqrt(MSE)
            corr = stats.spearmanr(obs_rounded,fcst_rounded)[0]
            
            if variable == "PCPT6":
                length = int(length/6)
            
            elif variable == "PCPT24":
                length = int(length/24)
            else:
                length = length            
            
            len_fcst = str(len(fcst_noNaNs)) + "/" + str(length)   
            numstations = str(num_stations) + "/" + str(totalstations)
                
            make_textfile(model, grid, input_domain, savetype, date_entry1, date_entry2, time_domain, variable, filepath, MAE, RMSE, corr, len_fcst, numstations)

def model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,hour,length,totalstations,time_domain,variable,filepath):
    if "ENS" in model:
        modelpath = model + '/'
    else:
        modelpath = model + '/' + grid + '/'

    if int(maxhour) >= hour:  
        if variable == "PCPT6":
            if int(maxhour) == int(hour):
                total_length = int(((length*(delta+1))/6)-(delta+1))
            else:
                total_length = int((length*(delta+1))/6)
        elif variable == "PCPT24":
            if int(maxhour) == int(hour):
                total_length = int(((length*(delta+1))/24)-(delta+1))
            else:
                total_length = int((length*(delta+1))/24)
        else:
            total_length = int(length*(delta+1))
                
        len_fcst = "0/" + str(total_length)
        numstations = "0/" + str(totalstations)
        
        
        f1 = open(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+")       
        read_f1 = np.loadtxt(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f1 and date_entry2 not in read_f1:
            f1.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f1.write("nan   ") #MAE
            f1.write(len_fcst + "   ")
            f1.write(numstations + "\n")
        
            f1.close()    
                
        
        f2 = open(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+")       
        read_f2 = np.loadtxt(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f2 and date_entry2 not in read_f2:
            f2.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f2.write("nan   ") #RMSE
            f2.write(len_fcst + "   ")
            f2.write(numstations + "\n")
            
            f2.close()  
            
        
        f3 = open(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",wm+"+") 
        read_f3 = np.loadtxt(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f3 and date_entry2 not in read_f3:
            f3.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f3.write("nan   ") #corr
            f3.write(len_fcst + "   ")
            f3.write(numstations + "\n")
            
            f3.close()  

def get_rankings(filepath, delta, input_domain, date_entry1, date_entry2, savetype, all_stations, station_df, variable, date_list, model, grid, maxhour, gridname, filehours, obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24):
    
  
    if os.path.isdir(textfile_folder +  filepath) == False:
        os.makedirs(textfile_folder +  filepath)
            
    # open the file for the current model and get all the stations from it
    model_df_name = model+gridname
    stations_in_domain = np.array(station_df.query(model_df_name+"==1")["Station ID"],dtype='str')

    #these variables will contain all the fcst and obs for the stations that exist for each model
    obs_allstations_180hr, fcst_allstations_180hr = [],[]
    obs_allstations_120hr, fcst_allstations_120hr = [],[]
    obs_allstations_84hr, fcst_allstations_84hr = [],[]
    obs_allstations_60hr, fcst_allstations_60hr = [],[]
    obs_allstations_day1, fcst_allstations_day1 = [],[]
    obs_allstations_day2, fcst_allstations_day2 = [],[]
    obs_allstations_day3, fcst_allstations_day3 = [],[]
    obs_allstations_day4, fcst_allstations_day4 = [],[]
    obs_allstations_day5, fcst_allstations_day5 = [],[]
    obs_allstations_day6, fcst_allstations_day6 = [],[]
    obs_allstations_day7, fcst_allstations_day7 = [],[]
    
    totalstations = 0
    num_stations = 0
    
    for station in stations_in_domain:

        if station not in all_stations:
            #print("   Skipping station " + station + ")
            continue

        if check_variable(variable, station, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24) == False:                  
            #print("   Skipping station " + station + " (no " + variable + " data)")
            continue
        
        if len(station) < 4:
            station = "0" +str(station)
        
        if check_dates(date_entry1, delta, filepath, variable, station) == False:
            print("   Skipping station " + station + " (not enough dates yet)")
            continue

        
        # total stations that should be included in each model/grid
        totalstations = totalstations+1
        '''
        #when using the "small" domain, only include raw data if KF data also exists at that hour
        if input_domain == "small" and variable in ["SFCTC","SFCWSPD"]:
            all_fcst_KF = get_fcst(station, filepath, variable + '_KF', date_list,filehours, date_entry1, date_entry2)
            fcst_final_all_KF = np.array(all_fcst_KF).T
            fcst_flat_all_KF = fcst_final_all_KF.flatten()
            
            if pd.isna(fcst_flat_all_KF).all() == True:    
                print("   Skipping station " + station + " (No KF data)")
                continue
            if pd.isna(fcst_flat_all_KF).any() == False:  
                all_fcst_KF = False
                print(station + " " + model + grid + " fcst KF missing")

        else:
        '''
        
        all_fcst_KF = False
            
        all_fcst = get_fcst(station, filepath, variable, date_list,filehours, date_entry1, date_entry2)    #goes to maxhour       
       
        fcst_final_all = np.array(all_fcst).T
        fcst_flat_all = fcst_final_all.flatten()
        
       
        if variable != "PCPT24":
            obs_flat_all = np.array(obs_df_180hr[station])
            
            #checks 180 hour only
            if pd.isna(fcst_flat_all).all() == True:    
                print("   Skipping station " + station + " (No forecast data)")
                continue
            
            if pd.isna(obs_flat_all).all() == True:    
                print("   Skipping station " + station + " (No obs data)")
                continue
        
        # total stations that ended up being included (doesn't count ones with no data)
        num_stations = num_stations+1
      
        if int(maxhour) >= 180 and variable!="PCPT24":
            fcst_NaNs_180hr, obs_flat_180hr = trim_fcst(all_fcst,obs_df_180hr,station,0,180,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)                            
            fcst_allstations_180hr.append(fcst_NaNs_180hr)
            obs_allstations_180hr.append(obs_flat_180hr)
            
         
        if int(maxhour) >= 168:        
            fcst_NaNs_day7,  obs_flat_day7  = trim_fcst(all_fcst,obs_df_day7,station,144,168,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_day7.append(fcst_NaNs_day7)
            obs_allstations_day7.append(obs_flat_day7)
            
        if int(maxhour) >= 144:
            fcst_NaNs_day6,  obs_flat_day6  = trim_fcst(all_fcst,obs_df_day6,station,120,144,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_day6.append(fcst_NaNs_day6)
            obs_allstations_day6.append(obs_flat_day6)
            
        if int(maxhour) >= 120:
            if variable!="PCPT24":
                fcst_NaNs_120hr, obs_flat_120hr = trim_fcst(all_fcst,obs_df_120hr,station,0,120,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
                fcst_allstations_120hr.append(fcst_NaNs_120hr)
                obs_allstations_120hr.append(obs_flat_120hr)
            
            fcst_NaNs_day5,  obs_flat_day5  = trim_fcst(all_fcst,obs_df_day5,station,96,120,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_day5.append(fcst_NaNs_day5)
            obs_allstations_day5.append(obs_flat_day5)

        if int(maxhour) >= 96:
            fcst_NaNs_day4,  obs_flat_day4  = trim_fcst(all_fcst,obs_df_day4,station,72,96,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_day4.append(fcst_NaNs_day4)
            obs_allstations_day4.append(obs_flat_day4)
            
        if int(maxhour) >= 84 and variable!="PCPT24":            
            fcst_NaNs_84hr,  obs_flat_84hr  = trim_fcst(all_fcst,obs_df_84hr,station,0,84,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_84hr.append(fcst_NaNs_84hr)
            obs_allstations_84hr.append(obs_flat_84hr)
            
        if int(maxhour) >= 72:
            fcst_NaNs_day3,  obs_flat_day3  = trim_fcst(all_fcst,obs_df_day3,station,48,72,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_day3.append(fcst_NaNs_day3)
            obs_allstations_day3.append(obs_flat_day3)
            
        if variable!="PCPT24":
            fcst_NaNs_60hr,  obs_flat_60hr  = trim_fcst(all_fcst,obs_df_60hr,station,0,60,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
            fcst_allstations_60hr.append(fcst_NaNs_60hr)
            obs_allstations_60hr.append(obs_flat_60hr)
                    
        fcst_NaNs_day1,  obs_flat_day1  = trim_fcst(all_fcst,obs_df_day1,station,0,24,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
        fcst_allstations_day1.append(fcst_NaNs_day1)
        obs_allstations_day1.append(obs_flat_day1)
        
        fcst_NaNs_day2,  obs_flat_day2  = trim_fcst(all_fcst,obs_df_day2,station,24,48,variable,filepath,date_list,filehours,all_fcst_KF,maxhour, delta, input_domain)  
        fcst_allstations_day2.append(fcst_NaNs_day2)
        obs_allstations_day2.append(obs_flat_day2)

    #sometimes theres no forecast data for a model
    if num_stations == 0:
        print("   NO FORECAST DATA FOR " + model + grid)
             
        if variable!="PCPT24":
            model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,180,180,totalstations,'180hr',variable,filepath)
            model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,120,120,totalstations,'120hr',variable,filepath)
            model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,84,84,totalstations,'84hr',variable,filepath)
            model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,60,60,totalstations,'60hr',variable,filepath)
        
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,168,24,totalstations,'day7',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,144,24,totalstations,'day6',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,120,24,totalstations,'day5',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,96,24,totalstations,'day4',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,72,24,totalstations,'day3',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,48,24,totalstations,'day2',variable,filepath)
        model_not_available(model, grid, delta, input_domain, date_entry1, date_entry2, savetype, maxhour,24,24,totalstations,'day1',variable,filepath)
        
    else:
    
        if variable!="PCPT24":
            get_statistics(delta, model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,180,180,fcst_allstations_180hr,obs_allstations_180hr,num_stations,totalstations,'180hr',variable,filepath)
            get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,120,120,fcst_allstations_120hr,obs_allstations_120hr,num_stations,totalstations,'120hr',variable,filepath)
            get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,84,84,fcst_allstations_84hr,obs_allstations_84hr,num_stations,totalstations,'84hr',variable,filepath)
            get_statistics(delta,model,grid, input_domain, savetype, date_entry1, date_entry2,maxhour,60,60,fcst_allstations_60hr,obs_allstations_60hr,num_stations,totalstations,'60hr',variable,filepath)

                
        get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,168,24,fcst_allstations_day7,obs_allstations_day7,num_stations,totalstations,'day7',variable,filepath)
        get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,144,24,fcst_allstations_day6,obs_allstations_day6,num_stations,totalstations,'day6',variable,filepath)
        get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,120,24,fcst_allstations_day5,obs_allstations_day5,num_stations,totalstations,'day5',variable,filepath)
        get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,96,24,fcst_allstations_day4,obs_allstations_day4,num_stations,totalstations,'day4',variable,filepath)
        get_statistics(delta,model, grid, input_domain, savetype, date_entry1, date_entry2,maxhour,72,24,fcst_allstations_day3,obs_allstations_day3,num_stations,totalstations,'day3',variable,filepath) 
        get_statistics(delta,model,grid, input_domain, savetype, date_entry1, date_entry2,maxhour,48,24,fcst_allstations_day2,obs_allstations_day2,num_stations,totalstations,'day2',variable,filepath)
        get_statistics(delta,model,grid, input_domain, savetype, date_entry1, date_entry2,maxhour,24,24,fcst_allstations_day1,obs_allstations_day1,num_stations,totalstations,'day1',variable,filepath)

def PCPT_obs_df_6(date_list_obs, delta, input_variable, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6,\
                  stations_with_PCPT24, all_stations, start_date, end_date):

    # get the hourly precip values
    obs_df_60hr_1,obs_df_84hr_1,obs_df_120hr_1,obs_df_180hr_1,obs_df_day1_1,obs_df_day2_1,obs_df_day3_1,obs_df_day4_1,obs_df_day5_1,obs_df_day6_1,obs_df_day7_1 = get_all_obs(delta, \
        stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24, all_stations, "PCPTOT", \
    start_date, end_date, date_list_obs)
    
    # grab the extra hour on the last outlook day
    obs_df_60hr_1 = obs_df_60hr_1.append(obs_df_180hr_1.iloc[60],ignore_index=True)
    obs_df_84hr_1 = obs_df_84hr_1.append(obs_df_180hr_1.iloc[84],ignore_index=True)
    obs_df_120hr_1 = obs_df_120hr_1.append(obs_df_180hr_1.iloc[ 120],ignore_index=True)
    obs_df_day1_1 = obs_df_day1_1.append(obs_df_180hr_1.iloc[24],ignore_index=True)
    obs_df_day2_1 = obs_df_day2_1.append(obs_df_180hr_1.iloc[48],ignore_index=True)
    obs_df_day3_1 = obs_df_day3_1.append(obs_df_180hr_1.iloc[72],ignore_index=True)
    obs_df_day4_1 = obs_df_day4_1.append(obs_df_180hr_1.iloc[96],ignore_index=True)
    obs_df_day5_1 = obs_df_day5_1.append(obs_df_180hr_1.iloc[120],ignore_index=True)
    obs_df_day6_1 = obs_df_day6_1.append(obs_df_180hr_1.iloc[144],ignore_index=True)
    obs_df_day7_1 = obs_df_day7_1.append(obs_df_180hr_1.iloc[168],ignore_index=True)
    
      
    # remove the first hour (0 UTC)
    obs_df_60hr_1 = obs_df_60hr_1.iloc[1:].reset_index(drop=True)
    obs_df_84hr_1 = obs_df_84hr_1.iloc[1:].reset_index(drop=True)
    obs_df_120hr_1 = obs_df_120hr_1.iloc[1:].reset_index(drop=True)
    obs_df_180hr_1 = obs_df_180hr_1.iloc[1:-5].reset_index(drop=True)
    obs_df_day1_1 = obs_df_day1_1.iloc[1:].reset_index(drop=True)
    obs_df_day2_1 = obs_df_day2_1.iloc[1:].reset_index(drop=True)
    obs_df_day3_1 = obs_df_day3_1.iloc[1:].reset_index(drop=True)
    obs_df_day4_1 = obs_df_day4_1.iloc[1:].reset_index(drop=True)
    obs_df_day5_1 = obs_df_day5_1.iloc[1:].reset_index(drop=True)
    obs_df_day6_1 = obs_df_day6_1.iloc[1:].reset_index(drop=True)
    obs_df_day7_1 = obs_df_day7_1.iloc[1:].reset_index(drop=True)
    
    
    # sum every 6 hours (1-6 UTC, 7-12 UTC etc). report NaN if any of the 6 hours is missing
    obs_df_60hr_1_trimmed = obs_df_60hr_1.groupby(obs_df_60hr_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_84hr_1_trimmed = obs_df_84hr_1.groupby(obs_df_84hr_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_120hr_1_trimmed = obs_df_120hr_1.groupby(obs_df_120hr_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_180hr_1_trimmed = obs_df_180hr_1.groupby(obs_df_180hr_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day1_1_trimmed = obs_df_day1_1.groupby(obs_df_day1_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day2_1_trimmed = obs_df_day2_1.groupby(obs_df_day2_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day3_1_trimmed = obs_df_day3_1.groupby(obs_df_day3_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day4_1_trimmed = obs_df_day4_1.groupby(obs_df_day4_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day5_1_trimmed = obs_df_day5_1.groupby(obs_df_day5_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day6_1_trimmed = obs_df_day6_1.groupby(obs_df_day6_1.index // 6).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day7_1_trimmed = obs_df_day7_1.groupby(obs_df_day7_1.index // 6).apply(pd.DataFrame.sum,skipna=False)


    #grab the 6-hr accum precip values
    obs_df_60hr_6,obs_df_84hr_6,obs_df_120hr_6,obs_df_180hr_6,obs_df_day1_6,obs_df_day2_6,obs_df_day3_6,obs_df_day4_6,obs_df_day5_6,\
        obs_df_day6_6,obs_df_day7_6 = get_all_obs(delta, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, \
                                                  stations_with_PCPT24, all_stations, "PCPT6", start_date, end_date, date_list_obs)
        
    # grab the extra hour on the last outlook day
    obs_df_60hr_6 = obs_df_60hr_6.append(obs_df_180hr_6.iloc[60],ignore_index=True)
    obs_df_84hr_6 = obs_df_84hr_6.append(obs_df_180hr_6.iloc[84],ignore_index=True)
    obs_df_120hr_6 = obs_df_120hr_6.append(obs_df_180hr_6.iloc[120],ignore_index=True)
    obs_df_day1_6 = obs_df_day1_6.append(obs_df_180hr_6.iloc[24],ignore_index=True)
    obs_df_day2_6 = obs_df_day2_6.append(obs_df_180hr_6.iloc[48],ignore_index=True)
    obs_df_day3_6 = obs_df_day3_6.append(obs_df_180hr_6.iloc[72],ignore_index=True)
    obs_df_day4_6 = obs_df_day4_6.append(obs_df_180hr_6.iloc[96],ignore_index=True)
    obs_df_day5_6 = obs_df_day5_6.append(obs_df_180hr_6.iloc[120],ignore_index=True)
    obs_df_day6_6 = obs_df_day6_6.append(obs_df_180hr_6.iloc[144],ignore_index=True)
    obs_df_day7_6 = obs_df_day7_6.append(obs_df_180hr_6.iloc[168],ignore_index=True)
    
    
    # remove all values except the ones every 6 hours (6 UTC, 12 UTC, etc. (skipping the first))
    obs_df_60hr_6_trimmed = obs_df_60hr_6.iloc[::6, :][1:].reset_index(drop=True) #grabs every 6 hours (skipping hour 0)
    obs_df_84hr_6_trimmed = obs_df_84hr_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_120hr_6_trimmed = obs_df_120hr_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_180hr_6_trimmed = obs_df_180hr_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day1_6_trimmed = obs_df_day1_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day2_6_trimmed = obs_df_day2_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day3_6_trimmed = obs_df_day3_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day4_6_trimmed = obs_df_day4_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day5_6_trimmed = obs_df_day5_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day6_6_trimmed = obs_df_day6_6.iloc[::6, :][1:].reset_index(drop=True)
    obs_df_day7_6_trimmed = obs_df_day7_6.iloc[::6, :][1:].reset_index(drop=True)
    
    
    #combine the obs from manually accumulating 6 hours from hourly, and the pre-calculated 6 hours
    obs_df_60hr_all = pd.concat([obs_df_60hr_1_trimmed, obs_df_60hr_6_trimmed],axis=1)
    obs_df_84hr_all = pd.concat([obs_df_84hr_1_trimmed, obs_df_84hr_6_trimmed],axis=1)
    obs_df_120hr_all = pd.concat([obs_df_120hr_1_trimmed, obs_df_120hr_6_trimmed],axis=1)
    obs_df_180hr_all = pd.concat([obs_df_180hr_1_trimmed, obs_df_180hr_6_trimmed],axis=1)
    obs_df_day1_all = pd.concat([obs_df_day1_1_trimmed, obs_df_day1_6_trimmed],axis=1)
    obs_df_day2_all = pd.concat([obs_df_day2_1_trimmed, obs_df_day2_6_trimmed],axis=1)
    obs_df_day3_all = pd.concat([obs_df_day3_1_trimmed, obs_df_day3_6_trimmed],axis=1)
    obs_df_day4_all = pd.concat([obs_df_day4_1_trimmed, obs_df_day4_6_trimmed],axis=1)
    obs_df_day5_all = pd.concat([obs_df_day5_1_trimmed, obs_df_day5_6_trimmed],axis=1)
    obs_df_day6_all = pd.concat([obs_df_day6_1_trimmed, obs_df_day6_6_trimmed],axis=1)
    obs_df_day7_all = pd.concat([obs_df_day7_1_trimmed, obs_df_day7_6_trimmed],axis=1)

    return(obs_df_60hr_all,obs_df_84hr_all,obs_df_120hr_all,obs_df_180hr_all,obs_df_day1_all,obs_df_day2_all,obs_df_day3_all,obs_df_day4_all,\
           obs_df_day5_all,obs_df_day6_all,obs_df_day7_all)

def PCPT_obs_df_24(date_list_obs, delta, input_variable, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, \
                   stations_with_PCPT24,all_stations,start_date, end_date):
    
    # get the hourly precip values
    _,_,_,obs_df_180hr_1,obs_df_day1_1,obs_df_day2_1,obs_df_day3_1,obs_df_day4_1,obs_df_day5_1,obs_df_day6_1,obs_df_day7_1 = \
        get_all_obs(delta, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24, \
                    all_stations, "PCPTOT", start_date, end_date, date_list_obs)
            
    # grab the extra hour on the last outlook day
    obs_df_day1_1 = obs_df_day1_1.append(obs_df_180hr_1.iloc[180*delta + 24],ignore_index=True)
    obs_df_day2_1 = obs_df_day2_1.append(obs_df_180hr_1.iloc[180*delta + 48],ignore_index=True)
    obs_df_day3_1 = obs_df_day3_1.append(obs_df_180hr_1.iloc[180*delta + 72],ignore_index=True)
    obs_df_day4_1 = obs_df_day4_1.append(obs_df_180hr_1.iloc[180*delta + 96],ignore_index=True)
    obs_df_day5_1 = obs_df_day5_1.append(obs_df_180hr_1.iloc[180*delta + 120],ignore_index=True)
    obs_df_day6_1 = obs_df_day6_1.append(obs_df_180hr_1.iloc[180*delta + 144],ignore_index=True)
    obs_df_day7_1 = obs_df_day7_1.append(obs_df_180hr_1.iloc[180*delta + 168],ignore_index=True)
    
    
    # remove the first hour (0 UTC)
    obs_df_day1_1 = obs_df_day1_1.iloc[1:].reset_index(drop=True)
    obs_df_day2_1 = obs_df_day2_1.iloc[1:].reset_index(drop=True)
    obs_df_day3_1 = obs_df_day3_1.iloc[1:].reset_index(drop=True)
    obs_df_day4_1 = obs_df_day4_1.iloc[1:].reset_index(drop=True)
    obs_df_day5_1 = obs_df_day5_1.iloc[1:].reset_index(drop=True)
    obs_df_day6_1 = obs_df_day6_1.iloc[1:].reset_index(drop=True)
    obs_df_day7_1 = obs_df_day7_1.iloc[1:].reset_index(drop=True)
  
    
    # sum every 6 hours (1-6 UTC, 7-12 UTC etc). report NaN if any of the 6 hours is missing
    obs_df_day1_1_trimmed = obs_df_day1_1.groupby(obs_df_day1_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day2_1_trimmed = obs_df_day2_1.groupby(obs_df_day2_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day3_1_trimmed = obs_df_day3_1.groupby(obs_df_day3_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day4_1_trimmed = obs_df_day4_1.groupby(obs_df_day4_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day5_1_trimmed = obs_df_day5_1.groupby(obs_df_day5_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day6_1_trimmed = obs_df_day6_1.groupby(obs_df_day6_1.index // 24).apply(pd.DataFrame.sum,skipna=False)
    obs_df_day7_1_trimmed = obs_df_day7_1.groupby(obs_df_day7_1.index // 24).apply(pd.DataFrame.sum,skipna=False)

     
    #grab the 6-hr accum precip values
    _,_,_,obs_df_180hr_24,obs_df_day1_24,obs_df_day2_24,obs_df_day3_24,obs_df_day4_24,obs_df_day5_24,obs_df_day6_24,obs_df_day7_24 = \
        get_all_obs(delta, stations_with_SFCTC, stations_with_SFCWSPD, stations_with_PCPTOT, stations_with_PCPT6, stations_with_PCPT24, \
                    all_stations, "PCPT24", start_date, end_date, date_list_obs)
        
    
    # grab the extra hour on the last outlook day
    obs_df_day1_24 = obs_df_day1_24.append(obs_df_180hr_24.iloc[24],ignore_index=True)
    obs_df_day2_24 = obs_df_day2_24.append(obs_df_180hr_24.iloc[48],ignore_index=True)
    obs_df_day3_24 = obs_df_day3_24.append(obs_df_180hr_24.iloc[72],ignore_index=True)
    obs_df_day4_24 = obs_df_day4_24.append(obs_df_180hr_24.iloc[96],ignore_index=True)
    obs_df_day5_24 = obs_df_day5_24.append(obs_df_180hr_24.iloc[120],ignore_index=True)
    obs_df_day6_24 = obs_df_day6_24.append(obs_df_180hr_24.iloc[144],ignore_index=True)
    obs_df_day7_24 = obs_df_day7_24.append(obs_df_180hr_24.iloc[168],ignore_index=True)
    
    # remove all values except the ones every 24 hours (24 (0) UTC, etc. (skipping the first))
    obs_df_day1_24_trimmed = obs_df_day1_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day2_24_trimmed = obs_df_day2_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day3_24_trimmed = obs_df_day3_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day4_24_trimmed = obs_df_day4_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day5_24_trimmed = obs_df_day5_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day6_24_trimmed = obs_df_day6_24.iloc[::24, :][1:].reset_index(drop=True)
    obs_df_day7_24_trimmed = obs_df_day7_24.iloc[::24, :][1:].reset_index(drop=True)
    
    #combine the obs from manually accumulating 6 hours from hourly, and the pre-calculated 6 hours
    obs_df_day1_all = pd.concat([obs_df_day1_1_trimmed, obs_df_day1_24_trimmed],axis=1)
    obs_df_day2_all = pd.concat([obs_df_day2_1_trimmed, obs_df_day2_24_trimmed],axis=1)
    obs_df_day3_all = pd.concat([obs_df_day3_1_trimmed, obs_df_day3_24_trimmed],axis=1)
    obs_df_day4_all = pd.concat([obs_df_day4_1_trimmed, obs_df_day4_24_trimmed],axis=1)
    obs_df_day5_all = pd.concat([obs_df_day5_1_trimmed, obs_df_day5_24_trimmed],axis=1)
    obs_df_day6_all = pd.concat([obs_df_day6_1_trimmed, obs_df_day6_24_trimmed],axis=1)
    obs_df_day7_all = pd.concat([obs_df_day7_1_trimmed, obs_df_day7_24_trimmed],axis=1)

    obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr = [],[],[],[]
    
    return(obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1_all,obs_df_day2_all,obs_df_day3_all,obs_df_day4_all,obs_df_day5_all,obs_df_day6_all,obs_df_day7_all)
     
