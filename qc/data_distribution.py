#!/usr/bin python

"""
Created in 2023
@author: reagan mckinney

"""

import os
import pandas as pd
import numpy as np
import datetime 
from datetime import date
from datetime import timedelta
import sys
import math
import copy
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from scipy import stats
import sqlite3
import matplotlib.pyplot as plt

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

domain = 'small' # choose small or large domain
variables = ['SFCTC', 'SFCWSPD', 'PCPTOT']

precip_threshold = 100
wind_threshold = 100
temp_min = -60
temp_max = 60 

station_df = pd.read_csv(station_file)

stations_with_SFCTC = np.array(station_df.query("SFCTC==1")["Station ID"],dtype=str)
stations_with_SFCWSPD = np.array(station_df.query("SFCWSPD==1")["Station ID"],dtype=str)
stations_with_PCPTOT = np.array(station_df.query("PCPTOT==1")["Station ID"],dtype=str)
stations_with_PCPT6 = np.array(station_df.query("PCPT6==1")["Station ID"],dtype=str)
stations_with_PCPT24 = np.array(station_df.query("PCPT24==1")["Station ID"],dtype=str)

today = date.today()
start = date(2021, 10, 1)
delta = today - start
tot_hours = delta.days * 24

###########################################################
### ---------------------- FUNCTIONS ----------------------
###########################################################

def get_station_data(variable):
    
    obs_all = []
    len_all = []
    
    if variable == 'SFCTC':
        station_list = copy.deepcopy(stations_with_SFCTC)              
    elif variable == 'SFCWSPD':  
        station_list = copy.deepcopy(stations_with_SFCWSPD) 

    elif variable == "PCPTOT":
        station_list = copy.deepcopy(stations_with_PCPTOT)  
    
    for station in station_list:
        
        if int(station) < 1000:
            station = '0' + str(station)

        sql_path = obs_filepath + variable + '/' + station + '.sqlite'
        sql_con = sqlite3.connect(sql_path)
        
        sql_query = "SELECT * from 'All'"
        obs = pd.read_sql_query(sql_query, sql_con)
        len_data = len(obs)
    
        obs_all.append(obs['Val'])
        len_all.append(len_data)
    return(obs_all, station_list, len_all)

def plot_station_data(obs_all, variable, station_list):

    fig, ax = plt.subplots(figsize=(50,6))
    plot = ax.boxplot(obs_all)
    ax.set(title = variable + " distribution for all stations " + domain + " domain")
    ax.set_xticklabels(station_list, rotation=90)
    
    if variable == 'SFCTC':
        plt.ylim([temp_min,temp_max])

    elif variable == 'PCPTOT':
        plt.ylim([0,precip_threshold])
    
    elif variable == 'SFCWSPD':
        plt.ylim([0,wind_threshold])

    plt.savefig('img/'+variable+'.png')

def data_quantity(station_list, len_all):
    for x in range(len(len_all)):
        percent_of_data = int(len_all[x]/tot_hours * 100)
        if percent_of_data < 90:
            print(station_list[x] + " contains " + str(percent_of_data) + "% data")

def contains_outliers(station_list, variable, obs_all):
    
    for x in range(len(obs_all)):
        if variable == 'SFCTC':
            if obs_all[x] > temp_max:
                print(station_list[x] + "recorded a temperature of " + obs_all[x] + "which exceeds the threshold")

            elif obs_all[x] < temp_min:
                  print(station_list[x] + "recorded a temperature of " + obs_all[x] + "which exceeds the threshold")
        
        elif variable == 'SFCWSPD':
            if obs_all[x] > wind_threshold:
                print(station_list[x] + "recorded a wind speed of " + obs_all[x] + "which exceeds the threshold")

        elif variable == 'PCPTOT':
            if obs_all[x] > precip_threshold:
                print(station_list[x] + "recorded a precipitation total of " + obs_all[x] + "which exceeds the threshold")

def main(args):

    for variable in variables:
        print("Now on ...... " + variable)
        obs_all, station_list, len_all = get_station_data(variable)
        plot_station_data(obs_all, variable, station_list)
        data_quantity(station_list, len_all)
        contains_outliers(station_list, variable, obs_all)

if __name__ == "__main__":
    main(sys.argv)
