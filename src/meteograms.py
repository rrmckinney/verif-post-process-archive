#!/usr/bin python

"""
Created in summer 2021

@author: evagnegy

This script creates a temp (raw and KF) in C, precip (hourly and accumulated) in mm, and wind speed (raw and KF) in km/hr
meterogram for a list of (currently 34) stations. It is in UTC. Input to this script is the first date on the plot you want
(currently it goes 8 days in the future) in YYMMDD and it starts at 00 UTC
 
This needs updated for the larger list of stations

"""

import matplotlib.pyplot as plt
import os
import numpy as np
from datetime import datetime, timedelta
import sys
import time
import math
import copy
import shutil
import pandas as pd

###########################################################
### -------------------- FILEPATHS ------------------------
###########################################################


#path to save the log/output
logfilepath = "/home/verif/verif-post-process/log/meteograms.log"

#location to save the images
save_folder = '/www_oper/results/verification/images/meteograms/'
#save_folder = '/scratch/egnegy/verification/python_plots/station_plots/'

#location where obs files are (all textfiles should be in this directory)
obs_filepath = "/verification/Observations/"

#location where forecast files are (immediately within this directory should be model folders, then grid folders, then the textfiles)
fcst_filepath = "/verification/Forecasts"

#description file for stations
station_file = '/home/verif/verif-get-data/input/station_list_meteograms.txt'

#description file for models
models_file = '/home/verif/verif-get-data/input/model_list.txt'

###########################################################
### -------------------- INPUT ------------------------
###########################################################

# takes an input date for the first date on the plot at 00Z, should be date (YYMMDD) 
if len(sys.argv) == 2:
    date_entry = sys.argv[1]    #input date YYMMDD
    start_date = str(date_entry) + '00'  
    input_date = datetime.strptime(start_date, "%y%m%d%H").date()

else:
    raise Exception("Invalid input entries. Needs YYMMDD for start plot date")



# list of strings of the station IDs
station_df = pd.read_csv(station_file)

stations = np.array(station_df.query("`Meteogram flag`==1")["Station ID"],dtype=str)

# info about the stations used in plot titles
stations_longname = np.array(station_df.query("`Meteogram flag`==1")["Station name"],dtype=str)
stations_shortname = np.array(station_df.query("`Meteogram flag`==1")["Station agency"],dtype=str)

# makes the plot titles from the long names, short names, and station IDs
station_names = [long + ' (' + short + ':' + ID + ')' for long, short, ID in zip(stations_longname, stations_shortname, stations)]

# variables to read and plot
variables = ['SFCTC_KF', 'SFCTC', 'PCPTOT', 'APCP', 'SFCWSPD_KF', 'SFCWSPD']
yaxis_labels = ['Temperature-KF [C]', 'Temperature-Raw [C]', 'Hourly Precipitation [mm]', 'Accumulated Precipitation [mm]', 'Wind Speed-KF [km/hr]', 'Wind Speed-Raw [km/hr]']


# =============================================================================
# # this section is for testing purposes 
# stations = ['3510']
# 
# # needs to be in same order as list above
# station_names = ['UBC ESB Rooftop (UBC:3510)']
# 
# variables = ['SFCTC_KF', 'SFCTC', 'PCPTOT', 'APCP', 'SFCWSPD_KF', 'SFCWSPD']
# yaxis_labels = ['Temperature-KF [C]', 'Temperature-Raw [C]', 'Hourly Precipitation [mm]', 'Accumulated Precipitation [mm]', 'Wind Speed-KF [km/hr]', 'Wind Speed-Raw [km/hr]']
# 
# =============================================================================


# list of model names as strings (names as they are saved in www_oper and my output folders)
models = np.loadtxt(models_file,usecols=0,dtype='str')

grids = np.loadtxt(models_file,usecols=1,dtype='str') #list of grid sizings (g1, g2, g3 etc) for each model
gridres = np.loadtxt(models_file,usecols=2,dtype='str') #list of grid resolution in km for each model
model_names = np.loadtxt(models_file,usecols=4,dtype='str') #longer names, used for legend

# this loop makes the names for each model/grid pair that will go in the legend
legend_labels = []
for i in range(len(model_names)):
    for grid in gridres[i].split(","):
        model = model_names[i]
        
        if model == "SREF":
            grid = ""
        legend_labels.append(model + grid)


#colors to plot, must be same length (or longer) than models
model_colors = ['C0','C1','C2','C3','C4','C5','C6','C7','C8','C9','#ffc219','#CDB7F6','#65fe08','#fc3232','#754200','#00FFFF','#fc23ba','#a1a1a1','#000000','#000000','#000000','#000000']
grid_lines = ["-","--",":","-.","-"] #reused g1 and g5 since no models have both grids

#these are the stations that only record precip data every 6 hours
precip6hrs_stations = ['597','604','583','600','606','601','607','610','612','603']

# num of days to go forward on plot - highest models go is 7.5 (but this must be an int, must be whole days)
days = 8    

###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################

# turns any missing data (valued at -999) into NaNs
# these plot as gaps 
# also turns bad data (unnaturally large values) into NaNs
def remove_missing_data(data):

    for i in range(len(data)):
        if data[i] == -999: 
            print("      removing datapoint: " + str(data[i]))
            data[i] = np.nan           
            
    return(data)

# this removes (NaNs) any fcst data where the obs is not recorded
def remove_missing_obs(fcst, obs):
    
    #gets whichever list is longer to loop through, since both are recorded every hour
    if len(fcst) > len(obs):
        length = len(obs)
    else:
        length = len(fcst)
        
    for i in range(length):        
        if math.isnan(obs[i]) == True:
            fcst[i] = np.nan
                
    return(fcst)

# a function that takens in a list and returns True if the list contains all consecutive numbers
# this is used to make sure theres no missing files (hours) for each set of variable/model/grid/station
def checkConsecutive(hours_list):
    return sorted(hours_list) == list(range(min(hours_list), max(hours_list)+1))


# this function accumulates fcst data every 6 hours (skipping the very first hour, 00Z)
# so there is a 6hr cum. sum at 6:00, 12:00, 18:00, 24:00 etc
# this function is only ran for the stations that collect 6hr data
def accum_6hr(fcst):
    
    # this line creates the sum for every 6 entries, skipping the first one
    fcst_acc6hr = np.add.reduceat(fcst[1:], np.arange(0, len(fcst[1:]), 6))

    # this statement is for lengths that aren't divisible by 6 (meaning theres a few extra hours at the end)
    if len(fcst[1:])%6 !=0:
        fcst_acc6hr = fcst_acc6hr[:-1]
        
    # creates an array of all Nans the size of the original fcst array, then fills in the 6 hr average every 6th hour
    newfcst = [np.nan] * len(fcst)
    j=0
    for i in range(len(fcst)):
        
        if i%6 == 0 and i!=0:
    
            newfcst[i] = fcst_acc6hr[j]
            j=j+1
            
    return(newfcst)


#function that checks if all values of an array are the same (used for dates)
def areEqual(arr):
   
    first = arr[0];
     
    for i in range(1, len(arr)):
        if (arr[i] != first):
            return False;
           
    return True;


# returns the fcst data for the given model/grid, as well as the forecast hours
def get_data(filepath, station, variable, obs):
    
    file_list, hours = [], []
    
    if variable == "APCP":
        variable = "PCPTOT"
    
    # goes through the entire directory for each model+grid and and 
    # pulls out a list of the ones for the given station+variable wanted
    # Also pulls out the forecast hours that were collected from the filenames
    for all_files in os.listdir(filepath):
        if all_files.startswith(station + "." + variable + "."):
            file_list.append(all_files)
            hours.append(all_files[-7:-4])
            
    
    file_list.sort() # sorts the forecast hour in the file, since the station and variable are constant
    hours.sort() # list of hours that exist from filenames
    hours_int = [int(i) for i in hours] # convert hours from str to int
    
    #raises an exception because this means an error in the get_fcst_data collection script that needs fixed
    if checkConsecutive(hours_int) == False:
        raise Exception("      WARNING: Missing output file (hours not consecutive) for " + station + "." + variable)
        
        
    # subtract one for plotting purposes because hour 001 is 00 UTC
    hours_int = np.subtract(hours_int,1)

    # reads the first column of the file, which is the list of dates YYMMDD00
    dates = np.loadtxt(filepath + file_list[0],usecols=0,dtype=str)
    index = list(dates).index(start_date) #finds the index of the first date we want to plot

    # loops through every hour file for each model
    fcst = []
    for i in range(len(file_list)):

        # gets the date thats at the index found above, but for each file
        date = str(np.loadtxt(filepath + file_list[i],usecols=0)[index])

        # makes sure that it is getting the correct date
        if date == start_date + ".0":

            # if the date is right, collect the forecast data
            fcst.append(float(np.loadtxt(filepath + file_list[i],usecols=1)[index]))
        
        else: # for now this is an exception because it likely means there was an error with the get_fcst_data script that needs fixed
            #fcst.append(np.nan)
            raise Exception("      WARNING: Wrong date for " + file_list[i] + ": should be " + start_date + " but is " + date)
            #print("      removing data point from that hour")
    
    
    #removes bad/missing data data
    fcst = remove_missing_data(fcst)
    
    # gets 6hr sums for precip for the stations that need it
    if station in precip6hrs_stations and "PCP" in variable:
        fcst = accum_6hr(fcst)

    # removes forecast data where there is no obs data
    fcst = remove_missing_obs(fcst, obs)

    return(hours_int, fcst)


# returns the obs data for the given station, as well as the hours
def get_obs(filepath,station,variable):    
     
     file_list = []
     
     # ignore the "KF" in the variable list, since its the same thing as the non-KF variable for obs
     if "_KF" in variable:
         variable = variable[:-3]
     if variable == "APCP":
         variable = "PCPTOT"
     
     # gets the list of hour files for the given station/variable
     for all_files in os.listdir(filepath):
         if all_files.startswith(station + "." + variable):
             file_list.append(all_files) # appends all files for that station/variable
             
 
     file_list.sort() # sorts the hours in the file list, since the station and variable are constant
      
     #list of all dates for hour 001 that obs has been collected for
     dates = np.loadtxt(filepath + file_list[0],usecols=0,dtype=str)

     # this means the user picked a date to plot that there is no obs for (or it was the wrong format)
     if start_date not in list(dates):
         raise Exception("Invalid start date: " + start_date  + " not in output data collected. Make sure it is YYMMDD.")
    
     index_start = list(dates).index(start_date) #gets the index of the start date you want 
    
     # how many days in the future the end date should be, subtracts one bc we count the start date
     delta = timedelta(days=days-1)
    
     # the last date we want to plot
     end_date = (input_date + delta).strftime("%y%m%d%H")
    
    
     if end_date in list(dates): 
         index_end = list(dates).index(end_date) # if the end date is in the list, get that index
     else:
         index_end = len(dates)-1 # if its not, just plot up til the last day of obs that exists
     
     all_obs = [] #this will be the variable that combines all of the days of obs we want (since they are saved separetly by day)
     prev_date = input_date-timedelta(days=1) # this is used to check that the dates being appended are consecutive and that none were missed
     
     # this loops through the amount of days you want to plot (bc of the way the obs files are structured)
     # date loop is one day at a time
     for date_loop in np.linspace(index_start,index_end,index_end-index_start+1):
         
         # collects all obs data for the given day
         obs = [(float(np.loadtxt(filepath + file_list[i],usecols=1)[int(date_loop)])) for i in range(len(file_list))]
         obs = remove_missing_data(obs)
         
         # appends that list of that days obs data from the previous iterations
         all_obs.append(obs) #makes all of the dates in one list
         
         # collects all of the dates for the given index
         all_start_dates = [(float(np.loadtxt(filepath + file_list[i],usecols=0)[int(date_loop)])) for i in range(len(file_list))]
         
         # makes sure its looking at the same date for every file, or else there's an error in the obs
         dates_equal = areEqual(all_start_dates)
         if dates_equal == False:
             raise Exception(" -- OBS WARNING: obs date used does not match all obs date -- ")

         # the date the loop is on
         current_date = int(all_start_dates[0])
       
         # converts it to a python date YYMMDD00
         current_date2 = datetime.strptime(str(current_date), "%y%m%d%H").date()

         # makes sure the days are consecutive
         if current_date2-timedelta(days=1) != prev_date:
             raise Exception(" -- OBS WARNING: obs missing a day in obs_files: missing " + (current_date2-timedelta(days=1)).strftime('%y%m%d') + '00')

         # will be used in the next iteration to make sure the next day is "tomorrow"
         prev_date = copy.deepcopy(current_date2)
        
     # starts at 0 bc 001 is 00 UTC
     hours = np.linspace(0,len(np.hstack((all_obs)))-1,len(np.hstack((all_obs))))

     #hstack just flattens the list of lists (horizontally stacks them)
     return(hours, np.hstack((all_obs)))


# checks if station/var exists 
# returns True if any files exists, False if not
# this is used since some model/grid pairs don't exist
def check_data_exists(filepath, station, variable):
    if variable == "APCP":
        variable = "PCPTOT"
        
    flag = False
    for all_files in os.listdir(filepath):    
        if station + "." + variable in all_files:
            flag=True
    return(flag)
       
        
# MAIN PLOTTING FUCNTION: returns one plot every time it is ran
def time_series(station, variable, ylabel, title):
      
        
    plt.figure(figsize=(18, 6), dpi=150)
    color_i = 0 #variable for plotting, loops through color array
    leg_count = 0 #variable for plotting 
    
    # gets the obs data and hours
    hrs_obs, obs = get_obs(obs_filepath,station,variable)

    # this allows a cumulative sum with Nans in it (it just ignores the Nans). 
    # Might want to change this process in the future
    if variable == "APCP":
        obs = np.array(obs)
        obs = obs*0 + np.nan_to_num(obs).cumsum()

                
    for i in range(len(models)):
        model = models[i] #loops through each model
        linetype_i = 0 #variable for plotting

        for grid in grids[i].split(","): #loops through each grid size for each model
        
            #ENS only has one grid (and its not saved in a g folder)
            if  model == "ENS":
                filepath = fcst_filepath + model + '/'
                gridname = ""
            else:
                filepath = fcst_filepath + model + '/' + grid + '/'
                gridname = "_" + grid
                        
            # if it can't find the folder for the model/grid pair 
            if not os.path.isdir(filepath):
                raise Exception("Missing grid/model pair (or wrong base filepath for" + model + gridname)
            
            #print("    Now on.. " + model + gridname + " " + variable)
                        
            # this checks if there is data at that station    
            if check_data_exists(filepath, station, variable) == False:
                print("Skipping " + model + gridname + " (No files for " + variable + " at station: " + station + ")")
                leg_count=leg_count+1 #increase if skipping model
                continue
            
            
            # gets the fcst data and hours
            hrs, fcst = get_data(filepath, station, variable, obs)

            if np.isnan(fcst).all():
                print("Skipping " + model + gridname + " (No data for " + variable + " at station: " + station + ")")
                leg_count=leg_count+1 #increase if skipping model
                continue
                
            # this allows a cumulative sum with Nans in it (it just ignores the Nans). 
            # Might want to change this process in the future
            if variable == "APCP":
                fcst = np.array(fcst)
                fcst = fcst*0 + np.nan_to_num(fcst).cumsum()

            #########################
            ##### plot the fcst #####
            #########################
            
            # this plots the stations that only have precip data every 6 hours
            # this also plots station 597 which only has wind data every 6 hours
            if (station in precip6hrs_stations and "PCP" in variable) or (station == "597" and "SFCWSPD" in variable):
                fcst = np.array(fcst)
                mask = np.isfinite(fcst)
                plt.plot(hrs[mask], fcst[mask], label=legend_labels[leg_count],color=model_colors[color_i],linestyle=grid_lines[linetype_i])
           
            else:
                plt.plot(hrs, fcst, label=legend_labels[leg_count],color=model_colors[color_i],linestyle=grid_lines[linetype_i])

            leg_count=leg_count+1 #increase every grid and model
            linetype_i=linetype_i+1 #increase every grid
        color_i=color_i+1 #only increase every model (not grid size)
 
    ########################
    ##### plot the obs #####
    ########################
    
    # this plots station 597 which only has wind data every 6 hours
    if station == "597" and "SFCWSPD" in variable:
        obs = np.array(obs)
        mask = np.isfinite(obs)
        plt.plot(hrs_obs[mask],obs[mask],color='k',marker='.',label='station obs',markersize=12,linewidth=1)
        
    # this plots the stations that only have precip data every 6 hours
    elif station in precip6hrs_stations and "PCP" in variable:
        obs = np.array(obs)
        mask = np.isfinite(obs)
        mask[0] = False #this ignores the first hour, 0 UTC because its a cumsum of the prev 6 hours (where theres no fcst data)
        plt.plot(hrs_obs[mask],obs[mask],color='k',marker='.',label='station obs',markersize=12,linewidth=1)
    else:
        plt.plot(hrs_obs,obs,color='k',marker='.',label='station obs',markersize=12,linewidth=1)


    # add a vertical line every 24 hours
    xposition = [i*24 for i in range(1,days)]
    for xc in xposition:
        plt.axvline(x=xc, color='k', linestyle='-')
            
    # x-axis limit is in hours       
    plt.xlim([0,days*2*12])
     
    plt.xlabel('Date in 2022 [UTC]',fontsize=18)
    plt.ylabel(ylabel,fontsize=18)
    plt.yticks(fontsize=18)
    
    # convert the initialization date to a python date
    date = datetime.strptime(start_date, "%y%m%d%H")
    delta = timedelta(hours=12) # time iteratization for the x axis
    
    # time labels on x axis
    labels = [(date + i*delta).strftime("%H:00\n%a %b %d") for i in range(0,days*2+1)]
    for i in range(len(labels)):
        if labels[i].startswith('00:00'):
            labels[i] = '00:00\n'
    plt.xticks([i*12 for i in range(0,days*2+1)], labels, fontsize=15)
            
    plt.title(title, fontsize=18) #title
    plt.legend(bbox_to_anchor=(1.065, -0.2),ncol=10) #legend location is hard coded to be beneath the plot
    
    plt.gca().yaxis.grid(True)
    
    # all plots are saved within a folder for their date YYMMDD within the main save folder
    if not os.path.exists(save_folder + start_date[:-2]):
        os.makedirs(save_folder + start_date[:-2]) #make this folder if it doesn't exist

    # save the figure
    plt.savefig(save_folder + start_date[:-2] + '/' + station + "_" + variable  + '.png',bbox_inches='tight')
   
    plt.close()
    
###########################################################
### -------------------- MAIN ------------------------
###########################################################

def main(args):
    t = time.time() #get how long it takes to run
    sys.stdout = open(logfilepath, "w") #opens log file
    
    stations_i = 0
    for station in stations: #loop through stations
        var_i = 0
        #print("#######################")
        print("Now on STATION: " + station)
        #print("#######################")
        for var in variables: #loop through variables
            
            # these stations don't have wind obs data
            if "SFCWSPD" in var and station in ['424', '416', '417']:
                print("    Skipping SFCWSPD for this station (no obs data)")
                var_i = var_i + 1
                continue
            
            # this station doesn't have precip obs data
            if 'PCP' in var and station in ['721']:
                print("    Skipping PCPTOT for this station (no obs data)")
                var_i = var_i + 1
                continue
            
            # make the plot
            time_series(station, var, yaxis_labels[var_i], station_names[stations_i])
            
            var_i = var_i + 1
            
        stations_i = stations_i+1



    ###### this part moves files from /www_oper/ to scratch if they're over a 30 days old
    last_month = (datetime.today()-timedelta(days=30)).strftime('%y%m%d')
    original = save_folder + last_month + '/'
    target = '/verification/plots/'
    
    if os.path.isdir(original) and os.path.isdir(target):

        shutil.move(original,target)
        
        print("\n\n MOVED " + original + " to " + target)


    elapsed = time.time() - t #closes log file
    
    
    print(elapsed)
    print("It took " + str(round(elapsed/60)) + " minutes to run")
    sys.stdout.close() #close log file

if __name__ == "__main__":
    main(sys.argv)
