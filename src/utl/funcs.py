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
station_file = '/home/verif/verif-get-data/input/station_list_master.txt'

#description file for models
models_file = '/home/verif/verif-get-data/input/model_list.txt'

#folder where the stats save
textfile_folder = '/verification/Statistics/'

###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################


# makes a list of the dates you want from start to end, used to make sure the models and obs have the right dates
# obs get their own list because it will be different days than the initializition dates from the models for anything
#   past hours 0-24
def listofdates(obs = False):
    if obs == False:
        start = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
        end = datetime.datetime.strptime(end_date, "%y%m%d%H").date()

    elif obs == True:
        startday = 0 #forhour 1
        endday = 7 #for hour 180
        
        start = datetime.datetime.strptime(start_date, "%y%m%d%H").date() + timedelta(days=startday)
        end = datetime.datetime.strptime(end_date, "%y%m%d%H").date() + timedelta(days=endday)
    
    numdays = (end-start).days 
    date_list = [(start + datetime.timedelta(days=x)).strftime("%y%m%d%H") for x in range(numdays+1)]

    return(date_list)

#lists the hour filenames that we are running for
def get_filehours(hour1,hour2):
    
    hours_list = []
    for i in range(hour1,hour2+1):
        if i < 10:
            hour = "00" + str(i)
        elif i < 100:
            hour = "0" + str(i)
        else:
            hour = str(i)
        
        hours_list.append(hour)
        
    return(hours_list)

# checks to see if the right amount of dates exist, which is used for when new models/stations are added
# default station exists for when a new model is added (instead of new station)
def check_dates(filepath, variable, station='3510'):
    
    flag = True

    if "PCPT" in variable:
        variable = "PCPTOT"
    
    sql_path = filepath + station + ".sqlite"
    print(sql_path)
    sql_con = sqlite3.connect(sql_path)
    cursor = sql_con.cursor()
    cursor.execute("SELECT DISTINCT Date from 'All'")
    sql_result = cursor.fetchall()
    sql_result = [x[0] for x in sql_result]

    if len(sql_result) < delta+1:
        print("    Not enough dates available for this model/station/variable")
        flag = False

    elif int(start_date) < (sql_result[0]):
        print("    Model collection started " + str(sql_result[0]) + ", which is after input start_date")
        flag = False
    
    cursor.close()
    return(flag)

def get_rankings(station_df, variable, date_list, model, grid, maxhour, gridname, filepath, filehours, obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7):
    
    model_filepath = model + '/' + grid + '/'
    
    #makes model/grid folder if it doesn't exist
    if os.path.isdir(textfile_folder +  model_filepath) == False:
        os.makedirs(textfile_folder +  model_filepath)
            
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

        if check_variable(variable, station) == False:                  
            #print("   Skipping station " + station + " (no " + variable + " data)")
            continue
    
        if check_dates(filepath, variable, station) == False:
            print("   Skipping station " + station + " (not enough dates yet)")
            continue

        
        # total stations that should be included in each model/grid
        totalstations = totalstations+1
         
        #when using the "small" domain, only include raw data if KF data also exists at that hour
        if input_domain == "small" and variable in ["SFCTC","SFCWSPD"]:
            all_fcst_KF = get_fcst(station, filepath, variable + '_KF', date_list,filehours)
            fcst_final_all_KF = np.array(all_fcst_KF).T
            fcst_flat_all_KF = fcst_final_all_KF.flatten()
            
            if np.isnan(fcst_flat_all_KF).all() == True:    
                print("   Skipping station " + station + " (No KF data)")
                continue
            if np.isnan(fcst_flat_all_KF).any() == False:  
                all_fcst_KF = False
                print(station + " " + model + grid + " fcst KF missing")

        else:
            all_fcst_KF = False
            

            
        all_fcst = get_fcst(station, filepath, variable, date_list,filehours)    #goes to maxhour       
       

        fcst_final_all = np.array(all_fcst).T
        fcst_flat_all = fcst_final_all.flatten()
        
        if input_variable != "PCPT24":
            obs_flat_all = np.array(obs_df_180hr[station])
            
    
            #checks 180 hour only
            if np.isnan(fcst_flat_all).all() == True:    
                print("   Skipping station " + station + " (No forecast data)")
                continue
            
            if np.isnan(obs_flat_all).all() == True:    
                print("   Skipping station " + station + " (No obs data)")
                continue
        
        # total stations that ended up being included (doesn't count ones with no data)
        num_stations = num_stations+1
        

        
        #day3, hr180, day4, day5, hr120, day6, day7, hr180
        #start = [48, 0,  72, 96,  0,   120, 144, 0]
        #end =   [72, 84, 96, 120, 120, 144, 168, 180]
      
        if int(maxhour) >= 180 and input_variable!="PCPT24":
            fcst_NaNs_180hr, obs_flat_180hr = trim_fcst(all_fcst,obs_df_180hr,station,0,180,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)                            
            fcst_allstations_180hr.append(fcst_NaNs_180hr)
            obs_allstations_180hr.append(obs_flat_180hr)
            
         
        if int(maxhour) >= 168:        
            fcst_NaNs_day7,  obs_flat_day7  = trim_fcst(all_fcst,obs_df_day7,station,144,168,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_day7.append(fcst_NaNs_day7)
            obs_allstations_day7.append(obs_flat_day7)
            
        if int(maxhour) >= 144:
            fcst_NaNs_day6,  obs_flat_day6  = trim_fcst(all_fcst,obs_df_day6,station,120,144,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_day6.append(fcst_NaNs_day6)
            obs_allstations_day6.append(obs_flat_day6)
            
        if int(maxhour) >= 120:
            if input_variable!="PCPT24":
                fcst_NaNs_120hr, obs_flat_120hr = trim_fcst(all_fcst,obs_df_120hr,station,0,120,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
                fcst_allstations_120hr.append(fcst_NaNs_120hr)
                obs_allstations_120hr.append(obs_flat_120hr)
            
            fcst_NaNs_day5,  obs_flat_day5  = trim_fcst(all_fcst,obs_df_day5,station,96,120,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_day5.append(fcst_NaNs_day5)
            obs_allstations_day5.append(obs_flat_day5)

        if int(maxhour) >= 96:
            fcst_NaNs_day4,  obs_flat_day4  = trim_fcst(all_fcst,obs_df_day4,station,72,96,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_day4.append(fcst_NaNs_day4)
            obs_allstations_day4.append(obs_flat_day4)
            
        if int(maxhour) >= 84 and input_variable!="PCPT24":            
            fcst_NaNs_84hr,  obs_flat_84hr  = trim_fcst(all_fcst,obs_df_84hr,station,0,84,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_84hr.append(fcst_NaNs_84hr)
            obs_allstations_84hr.append(obs_flat_84hr)
            
        if int(maxhour) >= 72:
            fcst_NaNs_day3,  obs_flat_day3  = trim_fcst(all_fcst,obs_df_day3,station,48,72,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_day3.append(fcst_NaNs_day3)
            obs_allstations_day3.append(obs_flat_day3)
            
        if input_variable!="PCPT24":
            fcst_NaNs_60hr,  obs_flat_60hr  = trim_fcst(all_fcst,obs_df_60hr,station,0,60,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
            fcst_allstations_60hr.append(fcst_NaNs_60hr)
            obs_allstations_60hr.append(obs_flat_60hr)
                    
        fcst_NaNs_day1,  obs_flat_day1  = trim_fcst(all_fcst,obs_df_day1,station,0,24,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
        fcst_allstations_day1.append(fcst_NaNs_day1)
        obs_allstations_day1.append(obs_flat_day1)
        
        fcst_NaNs_day2,  obs_flat_day2  = trim_fcst(all_fcst,obs_df_day2,station,24,48,variable,filepath,date_list,filehours,all_fcst_KF,maxhour)  
        fcst_allstations_day2.append(fcst_NaNs_day2)
        obs_allstations_day2.append(obs_flat_day2)

    #sometimes theres no forecast data for a model
    if num_stations == 0:
        print("   NO FORECAST DATA FOR " + model + grid)
        
        if input_variable!="PCPT24":
            model_not_available(maxhour,180,180,totalstations,'180hr',variable,model_filepath)
            model_not_available(maxhour,120,120,totalstations,'120hr',variable,model_filepath)
            model_not_available(maxhour,84,84,totalstations,'84hr',variable,model_filepath)
            model_not_available(maxhour,60,60,totalstations,'60hr',variable,model_filepath)

        model_not_available(maxhour,168,24,totalstations,'day7',variable,model_filepath)
        model_not_available(maxhour,144,24,totalstations,'day6',variable,model_filepath)
        model_not_available(maxhour,120,24,totalstations,'day5',variable,model_filepath)
        model_not_available(maxhour,96,24,totalstations,'day4',variable,model_filepath)
        model_not_available(maxhour,72,24,totalstations,'day3',variable,model_filepath)
        model_not_available(maxhour,48,24,totalstations,'day2',variable,model_filepath)
        model_not_available(maxhour,24,24,totalstations,'day1',variable,model_filepath)
        
    else:
        if input_variable!="PCPT24":
            get_statistics(maxhour,180,180,fcst_allstations_180hr,obs_allstations_180hr,num_stations,totalstations,'180hr',variable,model_filepath)
            get_statistics(maxhour,120,120,fcst_allstations_120hr,obs_allstations_120hr,num_stations,totalstations,'120hr',variable,model_filepath)
            get_statistics(maxhour,84,84,fcst_allstations_84hr,obs_allstations_84hr,num_stations,totalstations,'84hr',variable,model_filepath)
            get_statistics(maxhour,60,60,fcst_allstations_60hr,obs_allstations_60hr,num_stations,totalstations,'60hr',variable,model_filepath)

        
        get_statistics(maxhour,168,24,fcst_allstations_day7,obs_allstations_day7,num_stations,totalstations,'day7',variable,model_filepath)
        get_statistics(maxhour,144,24,fcst_allstations_day6,obs_allstations_day6,num_stations,totalstations,'day6',variable,model_filepath)
        get_statistics(maxhour,120,24,fcst_allstations_day5,obs_allstations_day5,num_stations,totalstations,'day5',variable,model_filepath)
        get_statistics(maxhour,96,24,fcst_allstations_day4,obs_allstations_day4,num_stations,totalstations,'day4',variable,model_filepath)
        get_statistics(maxhour,72,24,fcst_allstations_day3,obs_allstations_day3,num_stations,totalstations,'day3',variable,model_filepath) 
        get_statistics(maxhour,48,24,fcst_allstations_day2,obs_allstations_day2,num_stations,totalstations,'day2',variable,model_filepath)
        get_statistics(maxhour,24,24,fcst_allstations_day1,obs_allstations_day1,num_stations,totalstations,'day1',variable,model_filepath)
