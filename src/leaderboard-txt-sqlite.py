#!/usr/bin python

"""
Created in 2021
@author: evagnegy

Edited in June 2023 by rmckinney to ingest sql data 

Input: start date (YYMMDD), end date (YYMMDD), variable, domain size
    Start and end date must be 7 or 28-31 day stretch
    variable options: SFCTC_KF, SFCTC, PCPTOT, PCPT6, PCPT24, SFCWSPD_KF, SFCWSPD
    domain options: large, small
    
The stats round the obs and forecasts to one decimal before doing statistics 
    - this can be changed in the (get_statistics) function
    - obs vary from integers to two decimals while forecasts have two decimals
        - temperature is sometimes integers while wind is sometimes every 10º or even 45º
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
### ---------------------- INPUT --------------------------
###########################################################

# takes an input date for the first and last day you want calculations for, must be a range of 7 or 30 days apart
if len(sys.argv) == 5:
    date_entry1 = sys.argv[1]    #input date YYMMDD
    start_date = str(date_entry1) + '00'  
    input_startdate = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
    
    date_entry2 = sys.argv[2]    #input date YYMMDD
    end_date = str(date_entry2) + '00'  
    input_enddate = datetime.datetime.strptime(end_date, "%y%m%d%H").date()
    
    #subtract 6 to match boreas time, might need to change in future
    today = datetime.datetime.now() - datetime.timedelta(hours=6) 
    needed_date = today - datetime.timedelta(days=8) #might need to change to 7
    if input_startdate > needed_date.date():
        raise Exception("Date too recent. Need start date to be at least 8 days ago.")

    delta = (input_enddate-input_startdate).days

    if delta == 6: # 6 is weekly bc it includes the start and end date (making 7)
        print("Performing WEEKLY calculation for " + start_date + " to " + end_date)
        savetype = "weekly"
        
    elif delta == 27 or delta == 28 or delta == 29 or delta == 30: #27 or 28 for feb
        print("Performing MONTHLY calculation for " + start_date + " to " + end_date)
        savetype = "monthly"

    else:
        raise Exception("Invalid date input entries. Start and end date must be 7 or 28-31 days apart (for weekly and monthly stats) Entered range was: " + str(delta+1) + " days")


    input_variable = sys.argv[3]
    if input_variable not in ['SFCTC_KF', 'SFCTC', 'PCPTOT', 'PCPT6', 'PCPT24', 'SFCWSPD_KF', 'SFCWSPD']:
        raise Exception("Invalid variable input entries. Current options: SFCTC_KF, SFCTC, PCPTOT, PCPT6, PCPT24, SFCWSPD_KF, SFCWSPD. Case sensitive.")

    input_domain = sys.argv[4]
    if input_domain not in ['large','small']:
        raise Exception("Invalid domain input entries. Current options: large, small. Case sensitive.")

            
else:
    raise Exception("Invalid input entries. Needs 2 YYMMDD entries for start and end dates, a variable name, and domain size")



# list of model names as strings (names as they are saved in www_oper and my output folders)
models = np.loadtxt(models_file,usecols=0,dtype='str')
grids = np.loadtxt(models_file,usecols=1,dtype='str') #list of grid sizings (g1, g2, g3 etc) for each model
gridres = np.loadtxt(models_file,usecols=2,dtype='str') #list of grid resolution in km for each model
hours = np.loadtxt(models_file,usecols=3,dtype='str') #list of max hours for each model

station_df = pd.read_csv(station_file)

stations_with_SFCTC = np.array(station_df.query("SFCTC==1")["Station ID"],dtype=str)
stations_with_SFCWSPD = np.array(station_df.query("SFCWSPD==1")["Station ID"],dtype=str)
stations_with_PCPTOT = np.array(station_df.query("PCPTOT==1")["Station ID"],dtype=str)
stations_with_PCPT6 = np.array(station_df.query("PCPT6==1")["Station ID"],dtype=str)
stations_with_PCPT24 = np.array(station_df.query("PCPT24==1")["Station ID"],dtype=str)


if input_domain == "large":
    all_stations = np.array(station_df.query("`All stations`==1")["Station ID"],dtype=str)
else:
    all_stations = np.array(station_df.query("`Small domain`==1")["Station ID"],dtype=str)
    
###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################


# this removes (NaNs) any fcst data where the obs is not recorded, or fcst is -999
def remove_missing_data(fcst, obs):
            
    for i in range(len(fcst)):        
        if math.isnan(obs[i]) == True:
            fcst[i] = np.nan
            
        if fcst[i] == -999:
            fcst[i] = np.nan
            obs[i] = np.nan
                
    return(fcst,obs) 

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

def check_variable(variable, station):

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

""" No ldnger need as all obs now in same file

# this puts obs in the same format as the fcsts
# currently only works for hours 1-180
def reorder_obs(obs_old):
"
    obs = []
 
    for i in range(24): #1-24
        obs.append(obs_old[i][:delta+1])
        
    for i in range(24): #25-48
        obs.append(obs_old[i][1:delta+2])
        
    for i in range(24): #49-72
        obs.append(obs_old[i][2:delta+3])

    for i in range(24): #73-96
        obs.append(obs_old[i][3:delta+4])            
 
    for i in range(24): #97-120
        obs.append(obs_old[i][4:delta+5])      

    for i in range(24): #121-144
        obs.append(obs_old[i][5:delta+6])  

    for i in range(24): #145-168
        obs.append(obs_old[i][6:delta+7])  

    for i in range(12): #169-180
        obs.append(obs_old[i][7:delta+8])  
    
    return(obs)
"""
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

# returns the fcst data for the given model/grid
def get_fcst(station, filepath, variable, date_list,filehours):
    
        fcst = []
        
        if int(station) < 1000:
            station = "0" + str(station)

        if "PCPT" in variable:
            variable = "PCPTOT"

        # pulls out a list of the files for the given station+variable+hour wanted   
        
        sql_con = sqlite3.connect(filepath + station + ".sqlite")
        cursor = sql_con.cursor()
        sql_query = "SELECT * from 'All' WHERE date BETWEEN " + str(start_date) + " AND " + str(end_date)
        cursor.execute(sql_query)
        fcst = cursor.fetchall()
        fcst_result = [y[0] for y in fcst]

        cursor.close()
        return(fcst_result)


def get_all_obs(variable, date_list_obs):
    
    print("Reading observational dataframe for " + variable + ".. ")
    
    if variable == 'SFCTC_KF' or variable == 'SFCTC':
        station_list = copy.deepcopy(stations_with_SFCTC)              
    elif variable == 'SFCWSPD_KF' or variable == 'SFCWSPD':  
        station_list = copy.deepcopy(stations_with_SFCWSPD) 
    
    elif variable == "PCPTOT":
        if input_variable == "PCPT6":
            station_list = [st for st in stations_with_PCPTOT if st not in stations_with_PCPT6 ]
        elif input_variable == "PCPT24":
            station_list = [st for st in stations_with_PCPTOT if st not in stations_with_PCPT24 ]
        else:
            station_list = copy.deepcopy(stations_with_PCPTOT)        
    
    elif variable == "PCPT6":
        station_list = copy.deepcopy(stations_with_PCPT6) 
        variable = "PCPT6"
    
    elif variable == "PCPT24":
        station_list = copy.deepcopy(stations_with_PCPT24) 
        variable = "PCPT24"
        
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
    
    #extra_point_df = pd.DataFrame()
        
    for station in station_list:

        if int(station) < 1000:
            station = "0" + str(station)

        print("      Now on station " + station)
        
        if station not in all_stations:
            #print("   Skipping station " + station)
            continue
        
        obs = []

        if "PCPT" in input_variable:
            if check_dates(fcst_filepath + 'ENS/' + variable + '/fcst.t/', "PCPTOT", station) == False:
                print("   Skipping station " + station + " (not enough dates yet)")
                continue
        else:
            if check_dates(fcst_filepath + 'ENS/' + variable + '/fcst.t/', variable, station) == False:
                print("   Skipping station " + station + " (not enough dates yet)")
                continue
        
        obs_directory = obs_filepath
        
            
        sql_con = sqlite3.connect(obs_directory + variable + "/" + station + ".sqlite")
        cursor = sql_con.cursor()
        sql_query = "SELECT * from 'All' WHERE date BETWEEN " + str(start_date) + " AND " + str(end_date)
        cursor.execute(sql_query)
        obs = cursor.fetchall()
        all_obs = [r[2] for r in obs]    
        cursor.close()
        #want the 13th point (12 UTC) on day 8 (7.5) ..jk
        #extra_point = obs[12][delta+7]
        
      
        hr60_obs = all_obs[:60]     #84 x 7   (30) 
        hr84_obs = all_obs[:84]     #84 x 7   (30)     
        hr120_obs = all_obs[:120]   #120 x 7  (30) 
        day1_obs = all_obs[:24]     #24 x 7   (30)   
        day2_obs = all_obs[24:48]   #24 x 7   (30)   
        day3_obs = all_obs[48:72]   #24 x 7   (30)     
        day4_obs = all_obs[72:96]   #24 x 7   (30)  
        day5_obs = all_obs[96:120]  #24 x 7   (30)  
        day6_obs = all_obs[120:144] #24 x 7   (30)  
        day7_obs = all_obs[144:168] #24 x 7   (30)  
        
        final_obs_180hr = np.array(all_obs).T
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

def make_textfile(time_domain, var, model, MAE, RMSE, corr, len_fcst, numstations):
   
        
        
    f1 = open(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "MAE_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt","a+")       
    read_f1 = np.loadtxt(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "MAE_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f1 and date_entry2 not in read_f1:
        f1.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f1.write("%3.3f   " % (MAE))
        f1.write(len_fcst + "   ")
        f1.write(numstations + "\n")
    
        f1.close()    
            
    
    f2 = open(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "RMSE_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt","a+")       
    read_f2 = np.loadtxt(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "RMSE_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f2 and date_entry2 not in read_f2:
        f2.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f2.write("%3.3f   " % (RMSE))
        f2.write(len_fcst + "   ")
        f2.write(numstations + "\n")
        
        f2.close()  
    
    
    f3 = open(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "spcorr_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt","a+") 
    read_f3 = np.loadtxt(textfile_folder +  model + '/' + input_domain + '/' + var + '/' + "spcorr_" + savetype + "_" + var + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
    if date_entry1 not in read_f3 and date_entry2 not in read_f3:
        f3.write(str(date_entry1) + " " + str(date_entry2) + "   ")
        
        f3.write("%3.3f   " % (corr))
        f3.write(len_fcst + "   ")
        f3.write(numstations + "\n")
        
        f3.close()  


def trim_fcst(all_fcst,obs_df,station,start,end,variable,filepath,date_list,filehours,all_fcst_KF,maxhour):

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
    
    if "PCPT" in variable:
        #removes the last point from every day if its at the maxhour, since it doesnt exist for fcst
        if int(end)==int(maxhour): 
            
            if end==180:    
                oneday_length = int((len(obs_flat)+1)/(delta+1))
                obs_flat = np.delete(obs_flat, np.arange(0, obs_flat.size, oneday_length)[1:]-1)
            else:
                oneday_length = int((len(obs_flat))/(delta+1))
                obs_flat = np.delete(obs_flat, np.arange(0, obs_flat.size+oneday_length, oneday_length)[1:]-1)
    

    # removes (NaNs) fcst data where there is no obs
    fcst_NaNs,obs_NaNs = remove_missing_data(fcst_flat, obs_flat)  

    
    if input_domain == "small" and variable in ["SFCTC","SFCWSPD"] and all_fcst_KF != False:
        trimmed_fcst_KF = all_fcst_KF[start:end]   
        fcst_final_KF = np.array(trimmed_fcst_KF).T
        fcst_flat_KF = fcst_final_KF.flatten() 
        
        fcst_NaNs,_ = remove_missing_data(fcst_flat, fcst_flat_KF) 
    

    return(fcst_NaNs, obs_NaNs)


def get_statistics(maxhour,hour,length,fcst_allstations,obs_allstations,num_stations,totalstations,time_domain,variable,model_filepath):

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
            model_not_available(maxhour,hour,length,totalstations,time_domain,variable,model_filepath)
        
        else:
            MAE = mean_absolute_error(obs_rounded,fcst_rounded)
            MSE = mean_squared_error(obs_rounded,fcst_rounded)
            RMSE = math.sqrt(MSE)
            corr = stats.spearmanr(obs_rounded,fcst_rounded)[0]
            
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
            
            
            len_fcst = str(len(fcst_noNaNs)) + "/" + str(total_length)   
            numstations = str(num_stations) + "/" + str(totalstations)
                
            make_textfile(time_domain, variable, model_filepath, MAE, RMSE, corr, len_fcst, numstations)

def model_not_available(maxhour,hour,length,totalstations,time_domain,variable,model_filepath):

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
        
        f1 = open(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt","a+")       
        read_f1 = np.loadtxt(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f1 and date_entry2 not in read_f1:
            f1.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f1.write("nan   ") #MAE
            f1.write(len_fcst + "   ")
            f1.write(numstations + "\n")
        
            f1.close()    
                
        
        f2 = open(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt","a+")       
        read_f2 = np.loadtxt(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f2 and date_entry2 not in read_f2:
            f2.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f2.write("nan   ") #RMSE
            f2.write(len_fcst + "   ")
            f2.write(numstations + "\n")
            
            f2.close()  
            
        
        f3 = open(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt","a+") 
        read_f3 = np.loadtxt(textfile_folder +  model_filepath + '/' + input_domain + '/' + variable + '/' + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt",dtype=str)  
        if date_entry1 not in read_f3 and date_entry2 not in read_f3:
            f3.write(str(date_entry1) + " " + str(date_entry2) + "   ")
            
            f3.write("nan   ") #corr
            f3.write(len_fcst + "   ")
            f3.write(numstations + "\n")
            
            f3.close()  

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
def PCPT_obs_df_6(date_list_obs):
    
    # get the hourly precip values
    obs_df_60hr_1,obs_df_84hr_1,obs_df_120hr_1,obs_df_180hr_1,obs_df_day1_1,obs_df_day2_1,obs_df_day3_1,obs_df_day4_1,obs_df_day5_1,obs_df_day6_1,obs_df_day7_1 = get_all_obs('PCPTOT', date_list_obs)
        
    
    # grab the extra hour on the last outlook day
    obs_df_60hr_1 = obs_df_60hr_1.append(obs_df_180hr_1.iloc[180*delta + 60],ignore_index=True)
    obs_df_84hr_1 = obs_df_84hr_1.append(obs_df_180hr_1.iloc[180*delta + 84],ignore_index=True)
    obs_df_120hr_1 = obs_df_120hr_1.append(obs_df_180hr_1.iloc[180*delta + 120],ignore_index=True)
    #obs_df_180hr_1 = obs_df_180hr_1.append(extra_point_df,ignore_index=True)
    obs_df_day1_1 = obs_df_day1_1.append(obs_df_180hr_1.iloc[180*delta + 24],ignore_index=True)
    obs_df_day2_1 = obs_df_day2_1.append(obs_df_180hr_1.iloc[180*delta + 48],ignore_index=True)
    obs_df_day3_1 = obs_df_day3_1.append(obs_df_180hr_1.iloc[180*delta + 72],ignore_index=True)
    obs_df_day4_1 = obs_df_day4_1.append(obs_df_180hr_1.iloc[180*delta + 96],ignore_index=True)
    obs_df_day5_1 = obs_df_day5_1.append(obs_df_180hr_1.iloc[180*delta + 120],ignore_index=True)
    obs_df_day6_1 = obs_df_day6_1.append(obs_df_180hr_1.iloc[180*delta + 144],ignore_index=True)
    obs_df_day7_1 = obs_df_day7_1.append(obs_df_180hr_1.iloc[180*delta + 168],ignore_index=True)
    
      
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
    obs_df_60hr_6,obs_df_84hr_6,obs_df_120hr_6,obs_df_180hr_6,obs_df_day1_6,obs_df_day2_6,obs_df_day3_6,obs_df_day4_6,obs_df_day5_6,obs_df_day6_6,obs_df_day7_6 = get_all_obs(input_variable, date_list_obs)
        
    # grab the extra hour on the last outlook day
    obs_df_60hr_6 = obs_df_60hr_6.append(obs_df_180hr_6.iloc[180*delta + 60],ignore_index=True)
    obs_df_84hr_6 = obs_df_84hr_6.append(obs_df_180hr_6.iloc[180*delta + 84],ignore_index=True)
    obs_df_120hr_6 = obs_df_120hr_6.append(obs_df_180hr_6.iloc[180*delta + 120],ignore_index=True)
    #obs_df_180hr_6 = obs_df_180hr_6.append(extra_point_df,ignore_index=True)
    obs_df_day1_6 = obs_df_day1_6.append(obs_df_180hr_6.iloc[180*delta + 24],ignore_index=True)
    obs_df_day2_6 = obs_df_day2_6.append(obs_df_180hr_6.iloc[180*delta + 48],ignore_index=True)
    obs_df_day3_6 = obs_df_day3_6.append(obs_df_180hr_6.iloc[180*delta + 72],ignore_index=True)
    obs_df_day4_6 = obs_df_day4_6.append(obs_df_180hr_6.iloc[180*delta + 96],ignore_index=True)
    obs_df_day5_6 = obs_df_day5_6.append(obs_df_180hr_6.iloc[180*delta + 120],ignore_index=True)
    obs_df_day6_6 = obs_df_day6_6.append(obs_df_180hr_6.iloc[180*delta + 144],ignore_index=True)
    obs_df_day7_6 = obs_df_day7_6.append(obs_df_180hr_6.iloc[180*delta + 168],ignore_index=True)
    
    
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

    return(obs_df_60hr_all,obs_df_84hr_all,obs_df_120hr_all,obs_df_180hr_all,obs_df_day1_all,obs_df_day2_all,obs_df_day3_all,obs_df_day4_all,obs_df_day5_all,obs_df_day6_all,obs_df_day7_all)
      
def PCPT_obs_df_24(date_list_obs):
    
    # get the hourly precip values
    _,_,_,obs_df_180hr_1,obs_df_day1_1,obs_df_day2_1,obs_df_day3_1,obs_df_day4_1,obs_df_day5_1,obs_df_day6_1,obs_df_day7_1 = get_all_obs('PCPTOT', date_list_obs)
            
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
    _,_,_,obs_df_180hr_24,obs_df_day1_24,obs_df_day2_24,obs_df_day3_24,obs_df_day4_24,obs_df_day5_24,obs_df_day6_24,obs_df_day7_24 = get_all_obs("PCPT24", date_list_obs)
        
    
    # grab the extra hour on the last outlook day
    obs_df_day1_24 = obs_df_day1_24.append(obs_df_180hr_24.iloc[180*delta + 24],ignore_index=True)
    obs_df_day2_24 = obs_df_day2_24.append(obs_df_180hr_24.iloc[180*delta + 48],ignore_index=True)
    obs_df_day3_24 = obs_df_day3_24.append(obs_df_180hr_24.iloc[180*delta + 72],ignore_index=True)
    obs_df_day4_24 = obs_df_day4_24.append(obs_df_180hr_24.iloc[180*delta + 96],ignore_index=True)
    obs_df_day5_24 = obs_df_day5_24.append(obs_df_180hr_24.iloc[180*delta + 120],ignore_index=True)
    obs_df_day6_24 = obs_df_day6_24.append(obs_df_180hr_24.iloc[180*delta + 144],ignore_index=True)
    obs_df_day7_24 = obs_df_day7_24.append(obs_df_180hr_24.iloc[180*delta + 168],ignore_index=True)
    
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
      

def main(args):
    #sys.stdout = open(logfilepath, "w") #opens log file
    
    date_list = listofdates()
    date_list_obs = listofdates(obs=True)
          
    if input_variable == "PCPT6":       
        obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7 = PCPT_obs_df_6(date_list_obs)
    elif input_variable == "PCPT24":       
        obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7 = PCPT_obs_df_24(date_list_obs)
    else:
        obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7 = get_all_obs(input_variable, date_list_obs)
   
    for i in range(len(models)):
       model = models[i] #loops through each model
       
       for grid_i in range(len(grids[i].split(","))): #loops through each grid size for each model
           
           grid = grids[i].split(",")[grid_i]
           maxhour = hours[i].split(",")[grid_i] # the max hours that are in the current model/grid
           
           filehours = get_filehours(1, int(maxhour))
           #ENS only has one grid (and its not saved in a g folder)
           if "ENS" in model:
               filepath = fcst_filepath + model + '/' + input_variable + '/fcst.t/'
               gridname = ""
           elif model == "ENS_LR":
               filepath = fcst_filepath +model + '/' + input_variable + '/fcst.LR.t/'
           elif model == "ENS_hr":
               filepath = fcst_filepath +model + '/' + input_variable + '/fcst.hr.t/'
           elif model == "ENS_lr":
               filepath = fcst_filepath +model + '/' + input_variable + '/fcst.lr.t/'    
           elif model =="ENS_hr" and '_KF' in input_variable:
               filepath = fcst_filepath +model + '/' + input_variable + "fcst.hr.KF_MH.t/"  
           elif model =="ENS_lr" and '_KF' in input_variable:
               filepath = fcst_filepath +model + '/' + input_variable + "fcst.lr.KF_MH.t/"  
           elif model =="ENS_LR" and '_KF' in input_variable:
               filepath = fcst_filepath +model + '/' + input_variable + "fcst.LR.KF_MH.t/"          
           elif "_KF" in input_variable:
               filepath = fcst_filepath +model + '/' + grid + '/' + input_variable + "fcst.KF_MH.t/"          
               gridname = "_" + grid
           else:
               filepath = fcst_filepath + model + '/' + grid + '/' + input_variable + '/fcst.t/'
               gridname = "_" + grid
               

           if check_dates(filepath, input_variable) == False:
               print("   Skipping model " + model + gridname + " (check_dates flag)")
               continue
       
           # if it can't find the folder for the model/grid pair 
           if not os.path.isdir(filepath):
               raise Exception("Missing grid/model pair (or wrong base filepath for" + model + gridname)
           
           print("Now on.. " + model + gridname + " for " + input_variable)

           
           get_rankings(input_variable, date_list, model, grid, maxhour, gridname, filepath, filehours, obs_df_60hr,obs_df_84hr,obs_df_120hr,obs_df_180hr,obs_df_day1,obs_df_day2,obs_df_day3,obs_df_day4,obs_df_day5,obs_df_day6,obs_df_day7)

    #sys.stdout.close() #close log file

if __name__ == "__main__":
    main(sys.argv)
