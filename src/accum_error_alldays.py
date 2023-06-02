#!/usr/bin python

"""
Created in summer 2021

@author: evagnegy

This script creates a accumulated error plots. This script uses all available dates up until 6 days ago (so the 
   dates are equal between Day plots)
 
"""

import matplotlib.pyplot as plt
import os
import numpy as np
import datetime 
import sys
import time
import math
import warnings

#import matplotlib.cbook
#warnings.filterwarnings("ignore",category=matplotlib.cbook.mplDeprecation)
warnings.filterwarnings("ignore",category=RuntimeWarning)

###########################################################
### -------------------- FILEPATHS ------------------------
###########################################################


#path to save the log/output
logfilepath = "/home/verif/verif-post-process/log/accum_error.log"

#location to save the images
save_folder = '/www_oper/results/verification/images/accum_error/'
#save_folder = '/scratch/egnegy/verification/python_plots/station_plots/'

#location where obs files are (all textfiles should be in this directory)
obs_filepath = "/scratch/verif/verification/output/station_obs/"

#location where forecast files are (immediately within this directory should be model folders, then grid folders, then the textfiles)
fcst_filepath = "/scratch/verif/verification/output/model_fcsts/"

#description file for stations
station_file = '/home/verif/verif-get-data/input/station_list.txt'

#description file for models
models_file = '/home/verif/verif-get-data/input/model_list_temp.txt'

###########################################################
### -------------------- INPUT ------------------------
###########################################################

# =============================================================================
# # takes two input dates for the first and final date for the plots (needs to be YYMMDD)
# if len(sys.argv) == 3:
#     date_entry1 = sys.argv[1]    #input start date YYMMDD
#     date_entry2 = sys.argv[2]    #input end date YYMMDD
#     start_date = str(date_entry1) + '00'  
#     start_date2 = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
#     end_date = str(date_entry2) + '00' 
#     end_date2 = datetime.datetime.strptime(end_date, "%y%m%d%H").date()
# 
# else:
#     raise Exception("Invalid input entries. Needs two dates (YYMMDD) for a start and end date")
# =============================================================================

# takes one input date2 for final date for the plots (needs to be YYMMDD)
# currently the start date is just when the data starts, which is define in main()
if len(sys.argv) == 2:
    date_entry = sys.argv[1]    #input end date YYMMDD
    end_date = str(date_entry) + '00' 
    end_date2 = datetime.datetime.strptime(end_date, "%y%m%d%H").date()

else:
    raise Exception("Invalid input entries. Needs two dates (YYMMDD) for a start and end date")
    


#end_date = '21081800' #for data ending on the 24th 
#end_date2 = datetime.datetime.strptime(end_date, "%y%m%d%H").date()



# =============================================================================
# # list of strings of the station IDs
# stations = np.loadtxt(station_file,usecols=0,delimiter=',',dtype='str')
# 
# # info about the stations used in plot titles
# stations_longname = np.loadtxt(station_file,usecols=1,delimiter=',',dtype='str')
# stations_shortname = np.loadtxt(station_file,usecols=2,delimiter=',',dtype='str')
# 
# # makes the plot titles from the long names, short names, and station IDs
# station_names = [long + ' (' + short + ':' + ID + ')' for long, short, ID in zip(stations_longname, stations_shortname, stations)]
# =============================================================================



# this section is for testing purposes 
stations = ['3510']

# needs to be in same order as list above
station_names = ['UBC ESB Rooftop (UBC_RS:3510)']




variables = ['SFCTC_KF', 'SFCTC', 'PCPTOT', 'APCP', 'SFCWSPD_KF', 'SFCWSPD']
yaxis_labels = ['Temperature-KF Accumulated Absolute Error [C]', 'Temperature-Raw Accumulated Absolute Error [C]', \
                'Hourly Precipitation Accumulated Absolute Error [mm]', 'Daily Precipitation Accumulated Absolute Error [mm]',\
                'Wind Speed-KF Accumulated Absolute Error [km/hr]', 'Wind Speed-Raw Accumulated Absolute Error [km/hr]']

# =============================================================================
# variables = ['PCPTOT','APCP']
# yaxis_labels = ['Hourly Precipitation Accumulated Absolute Error [mm]','Daily Precipitation Accumulated Absolute Error [mm]']
# =============================================================================

# =============================================================================
# variables = ['SFCWSPD_KF', 'SFCWSPD']  
# yaxis_labels = ['Wind Speed-KF Accumulated Absolute Error [km/hr]', 'Wind Speed-Raw Accumulated Absolute Error [km/hr]']
# 
# =============================================================================

# list of model names as strings (names as they are saved in www_oper and my output folders)
models = np.loadtxt(models_file,usecols=0,dtype='str')

grids = np.loadtxt(models_file,usecols=1,dtype='str') #list of grid sizings (g1, g2, g3 etc) for each model
gridres = np.loadtxt(models_file,usecols=2,dtype='str') #list of grid resolution in km for each model
lasthours = np.loadtxt(models_file,usecols=3,dtype='str') #list of last hour for each model
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
model_colors = ['C0','C1','C2','#8a1a1a','C4','C5','C6','C7','C8','C9','#ffc219','#CDB7F6','#65fe08','#fc3232','#00FFFF','#fc23ba','#000000']
grid_lines = ["-","--",":","-.","-"] #reused g1 and g5 since no models have both grids

#these are the stations that only record precip data every 6 hours
precip6hrs_stations = ['597','604','583','600','606','601','607','610','612','603']


#outlook days to calculate error for and plot
outlook_days = ["1","2","3","4","5","6","7"] 

###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################

# =============================================================================
# # turns any missing data (valued at -999) into NaNs
# # these plot as gaps 
# # also turns bad data (unnaturally large values) into NaNs
# def remove_missing_data(data):
# 
#     for i in range(len(data)):
#         if data[i] == -999 or abs(data[i]) > 10000: 
#             print("      removing datapoint: " + str(data[i]))
#             data[i] = np.nan           
#             
#     return(data)
# 
# # this removes (NaNs) any fcst data where the obs is not recorded
# def remove_missing_obs(fcst, obs):
#     
#     #gets whichever list is longer to loop through, since both are recorded every hour
#     if len(fcst) > len(obs):
#         length = len(obs)
#     else:
#         length = len(fcst)
#         
#     for i in range(length):        
#         if math.isnan(obs[i]) == True:
#             fcst[i] = np.nan
#                 
#     return(fcst)
# =============================================================================

# makes a list of the dates you want from start to end, used to make sure the models and obs have the right dates
def listofdates(start_date):
    start = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
    end = datetime.datetime.strptime(end_date, "%y%m%d%H").date()
    numdays = (end-start).days

    date_list = [(start + datetime.timedelta(days=x)).strftime("%y%m%d%H") for x in range(numdays+1)]

    return(date_list)

# same function as above but different output type (here it's used for the x axis plotting)
def listofdates_plot(start_date):
    start = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
    end = datetime.datetime.strptime(end_date, "%y%m%d%H").date()
    numdays = (end-start).days

    date_list = [start + datetime.timedelta(days=x) for x in range(numdays+1)]

    return(date_list)


def check_NaNs(fcst_data,model,grid,day,dates,variable):
    

    missing_ALL_days = list(np.where(np.isnan(fcst_data).all(axis=0))[0])
    missing_ALL_hours = np.where(np.isnan(fcst_data).all(axis=1))[0]
    
    nan_ind = np.isnan(fcst_data)
    
    if len(missing_ALL_hours) > 0:
        print("  " + model + grid + "  Missing hour(s) " + str(missing_ALL_hours+1) + " for ALL dates for outlook day: " + day + " for (" + variable + ")")
    
    missing_date_list = []
    if len(missing_ALL_days) > 0:
        for i in missing_ALL_days:
            missing_date_list.append(dates[i].strftime("%m/%d"))
            
        print("  " + model + grid + "  Missing date(s) " + str(missing_date_list) + " for ALL hours for outlook day: " + day + " for (" + variable + ")")
    
    
    for i in range(np.shape(nan_ind)[0]): #len should always be 24
        for j in range(np.shape(nan_ind)[1]): #len should always be 24
            if(nan_ind[i][j]) == True and nan_ind[:,j].all() == False and nan_ind[i,:].all() == False:
                print("  " + model + grid + "  Missing hour " + str(i+1) + " for " + str(dates[j].strftime("%m/%d")) + " for outlook day: " + day + " for (" + variable + ")")
    
    return()

# returns the fcst data for the given model/grid
def get_fcst(filepath, station, variable, maxhour, index_start, index_end, date_list):
    
    file_list, fcst, dailyavg_fcst = [],[],[]
        
    if variable == "APCP":
        variable = "PCPTOT"
    # gets the list of hourly files for the given station/variable
    for all_files in os.listdir(filepath):
        if all_files.startswith(station + "." + variable + "."):
            file_list.append(all_files) # appends all files for that station/variable
             
    file_list.sort() # sorts the hours in the file list, since the station and variable are constant
     
    # makes sure there is the right amount of files for that model/grid
    if len(file_list) != maxhour:
        raise Exception(filepath + " missing a file for " + station + "." + variable + " (not " + str(maxhour) + " files)")
        
    for file in file_list:
        # reads the dates before collecting the obs, to ensure its the right dates we want to average
        dates_check = list(np.loadtxt(filepath + file,usecols=0,dtype=str)[index_start:index_end+1])
        if dates_check != date_list:
            raise Exception("fcst error: " + filepath + file + " has the wrong dates")
        
        # collects all fcst data for the given dates range
        # contains a list for every hour, each containing all of the wanted dates for that hour
        fcst.append((np.loadtxt(filepath + file,usecols=1)[index_start:index_end+1]))
       
    for day in range(0,7):
        if maxhour >= (day+1)*24:
            
            dailyavg_fcst.append(fcst[day*24:(day+1)*24])  

    return(dailyavg_fcst)  #this will be the length of full days that exists for each model (60 hours = 2 full days)


# returns the obs data for the given station, as well as the hours
def get_obs(station,variable, index_start, index_end, date_list):   
    
    #KF variables are the same as raw for obs
    if "_KF" in variable:
        variable = variable[:-3]
    if variable == "APCP":
        variable = "PCPTOT"
        
    file_list,obs = [],[]
    
    # gets the list of hour files for the given station/variable
    for all_files in os.listdir(obs_filepath):
        if all_files.startswith(station + "." + variable):
            file_list.append(all_files) # appends all files for that station/variable
             
    file_list.sort() # sorts the hours in the file list, since the station and variable are constant
     
    if len(file_list) != 24:
        raise Exception("Station " + station + " doesn't have 24 hours of files (000-023)")
    
    for file in file_list:
        # reads the dates before collecting the obs, to ensure its the right dates we want to average
        dates_check = list(np.loadtxt(obs_filepath + file,usecols=0,dtype=str)[index_start:index_end+1])
        if dates_check != date_list:
            raise Exception("OBS error: " + file + " has the wrong dates")
        
        # collects all obs for each hour, for the list of days specified by indexes
        # the +1 is for indexing purposes and the +6 is to collect the extra days to use for day 2-7 output
        obs.append(np.loadtxt(obs_filepath + file,usecols=1)[index_start:index_end+1+6])
        
        
        # at this point, obs has 24 lists (one for each hour, each with an entry for each day in the index list)
    
    # this averages hours 0-23 for each day, so the output is an average for each day in the index list
   # dailyavg_obs = np.mean(obs,axis=0)

    
    return(obs)


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
      
        
# this function gets the data for all of the models
def get_data(station, variable, index_start, index_end, date_list):
    
    fcst = []
    for i in range(len(models)):
        model = models[i] #loops through each model
        grid_i = 0 #counts the grid type
        for grid in grids[i].split(","): #loops through each grid size for each model
            
            maxhour = lasthours[i].split(",")[grid_i] #gets the max hour for each model/grid
        
            #ENS only has one grid (and its not saved in a g folder)
            if (model == "ENS"):
                filepath = fcst_filepath + model + '/' + variable + '/'
            else:
                filepath = fcst_filepath + model + '/' + grid + '/' + variable + '/'
                             
            if check_data_exists(filepath, station, variable) == False:
                print("  Skipping " + model + grid + " (No files)")
                grid_i=grid_i+1 #increase every grid
                continue
            
              
            
            forecast = get_fcst(filepath, station, variable, int(maxhour), index_start, index_end, date_list)
            
            
            # runs it once for each model/grid and collects all the days averages
            fcst.append(forecast)

            grid_i=grid_i+1 #increase every grid
 
    return(fcst)
    
 
# MAIN PLOTTING FUCNTION: returns one plot every time it is ran            
def plot(station, variable, ylabel, stationtitle, obs, fcst, day,start_date):

    dates = listofdates_plot(start_date)    

    plt.figure(figsize=(18, 10), dpi=100)
    color_i = 0 #variable for plotting
    leg_count = 0 #variable for plotting 
        
    for i in range(len(models)):
        model = models[i] #loops through each model
        linetype_i = 0 #variable for plotting

        for grid in grids[i].split(","): #loops through each grid size for each model
            
            maxhour = lasthours[i].split(",")[linetype_i] #gets the max hour for each model/grid
            
            #ENS only has one grid (and its not saved in a g folder)
            if (model == "ENS"):
                filepath = fcst_filepath + model + '/' + variable + '/'
            else:
                filepath = fcst_filepath + model + '/' + grid + '/' + variable + '/'
                        
            if check_data_exists(filepath, station, variable) == False:
                print("  Skipping " + model + grid + " (No files)")
                leg_count=leg_count+1 #increase if skipping model
                continue
            
            #skip models that don't have enough hours for the current day avg
            if int(maxhour) < day*24:
                leg_count=leg_count+1 #increase if skipping model
                continue

            

            # this gets all of the points (dates_list) for the current model/grid/day
            # its contains a list for all hours (24) where each hour list has the amount of days we are looking at
            fcst_outlookday = fcst[leg_count][day-1] 

            check_NaNs(fcst_outlookday,model,grid,str(day),np.array(dates),variable)
                  
            fcst_plot = fcst_outlookday[:]

            #print(model + grid + " day " + str(day))
            #print(fcst_plot)
            
            obs_plot=[]
            
            if day == 7:
                for x in range(0,24):
                    obs_plot.append(obs[x][6:])
            else:
                for x in range(0,24):
                    obs_plot.append(obs[x][(day-1):-6+(day-1)])
             
            if variable == "APCP":
                daily_acc_fcst = np.sum(fcst_plot,axis=0)
                daily_acc_obs = np.sum(obs_plot,axis=0)
                #bias = np.cumsum(np.abs(np.subtract(daily_acc_fcst,daily_acc_obs)))
                
                bias_missingdays = np.array(np.abs(np.subtract(daily_acc_fcst,daily_acc_obs)))
                
                bias = np.array(bias_missingdays)
                bias = bias*0 + np.nan_to_num(bias).cumsum()
                
                
            else:
                # this is the cumsum of the mean of all 24 hours of abs errors each day
                        #finds the abs error for each 24 hours, then mean that abs error
                #bias = np.cumsum(np.nanmean(np.abs(np.subtract(fcst_plot,obs_plot)),axis=0))
                
                bias_missingdays = np.array(np.nanmean(np.abs(np.subtract(fcst_plot,obs_plot)),axis=0))
                
                bias = np.array(bias_missingdays)
                bias = bias*0 + np.nan_to_num(bias).cumsum()

            
            #plot the forecast/models
            plt.plot(np.linspace(1,len(bias),len(bias)), bias, label=legend_labels[leg_count],color=model_colors[color_i],linestyle=grid_lines[linetype_i])

    
            leg_count=leg_count+1 #increase every grid and model
            linetype_i=linetype_i+1 #increase every grid
        color_i=color_i+1 #only increase every model (not grid size)
     
        
        
     
    dates_labels = [datetime.datetime.strftime(d,"%m-%d") for d in dates]
        
    #the [::2] shows only every other date    
    plt.xticks(np.linspace(1,len(dates_labels),len(dates_labels))[::2], dates_labels[::2], fontsize=18, rotation=45)   
         
    plt.xlabel('Initialization Date in 2021 [UTC]',fontsize=18)
    plt.ylabel(ylabel,fontsize=20)
    plt.yticks(fontsize=20)
               
    plt.title(stationtitle + " - Day " + str(day), fontsize=25)
    plt.legend(bbox_to_anchor=(1.065, -0.2),ncol=10)
      
    plt.gca().yaxis.grid(True)
    
    plt.savefig(save_folder + station + "_" + variable + '_day_' + str(day) + '.png',bbox_inches='tight')

    plt.close()            

  
  
###########################################################
### -------------------- MAIN ------------------------
###########################################################

def main(args):
    t = time.time() #get how long it takes to run
    sys.stdout = open(logfilepath, "w")
    
    
    #this is the list of dates we have data for (3510 is a reliable station)
    sample_dates = np.loadtxt(obs_filepath + '3510.SFCTC_OBS.001.txt',usecols=0,dtype=str)
    
    index_end = list(sample_dates).index(end_date)
     
    var_i = 0
    for var in variables:
        
                
        if "SFCWSPD" in var:
            start_date = '21072000'
        else:
            start_date = '21071600'
            
        index_start = list(sample_dates).index(start_date)
        date_list = listofdates(start_date)

        stations_i = 0
        #print("Now on VAR: " + var)
        for station in stations:
            print("  Now on STATION: " + station + " VAR: " + var)

            if "SFCWSPD" in var and station in ['424', '416', '417']:
               print("    Skipping SFCWSPD for this station (no obs data)")
               var_i = var_i + 1
               continue

            # add 6 to the end date
            obs = get_obs(station,var,index_start, index_end, date_list)
            
            fcst = get_data(station,var,index_start, index_end, date_list)
            
            for day in outlook_days:  

                plot(station, var, yaxis_labels[var_i], station_names[stations_i],obs,fcst,int(day),start_date)


            stations_i = stations_i+1
       
        var_i = var_i + 1
  
    elapsed = time.time() - t
    print(elapsed)
    print("It took " + str(round(elapsed/60)) + " minutes to run")
    sys.stdout.close() #close log file
if __name__ == "__main__":
    main(sys.argv)


