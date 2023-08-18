#!/usr/bin python

#!/usr/bin python

"""
Created in summer 2023

@author: Reagan McKinney

This script is for the weekly leadership board for categorical stats (precip & wind)
 
"""

import matplotlib.pyplot as plt
import os
import numpy as np
import pandas as pd
import datetime #import datetime, timedelta
import sys
from sklearn import preprocessing
from math import exp
import warnings
warnings.filterwarnings("ignore",category=RuntimeWarning)
###########################################################
### -------------------- FILEPATHS ------------------------
###########################################################


#path to save the log/output
#logfilepath = "/home/egnegy/python_plotting/log/plot_leaderboards.log"

#location to save the images for the website
#save_folder = '/www/results/verification/images/leaderboards/'

#location to save the images internally
save_folder = "/home/verif/verif-post-process/src/img/"

#description file for stations
station_file = '/home/verif/verif-post-process/input/station_list.txt'

#description file for models
models_file = '/home/verif/verif-post-process/input/model_list.txt'

textfile_folder = '/verification/Statistics/'

###########################################################
### -------------------- INPUT ------------------------
###########################################################

# takes an input date for the last day of the week you want to include
if len(sys.argv) == 4:
    date_entry1 = sys.argv[1]    #input date YYMMDD
    start_date = str(date_entry1) + '00'  
    input_startdate = datetime.datetime.strptime(start_date, "%y%m%d%H").date()
    print_startdate = datetime.datetime.strftime(input_startdate,"%m/%d/%y")
    
    date_entry2 = sys.argv[2]    #input date YYMMDD
    end_date = str(date_entry2) + '00'  
    input_enddate = datetime.datetime.strptime(end_date, "%y%m%d%H").date()
    print_enddate = datetime.datetime.strftime(input_enddate,"%m/%d/%y")

    delta = (input_enddate-input_startdate).days

    if delta == 6: # 6 is weekly bc it includes the start and end date (making 7)
        print("Performing WEEKLY calculation for " + start_date + " to " + end_date)
        savetype = "weekly"
        
    elif delta == 27 or delta == 28 or delta == 29 or delta == 30: # 29 is monthly bc it includes the start and end date (making 30)
        print("Performing MONTHLY calculation for " + start_date + " to " + end_date)
        savetype = "monthly"

    else:
        raise Exception("Invalid date input entries. Start and end date must be 7 or 28/29/30/31 days apart (for weekly and monthly stats) Entered range was: " + str(delta+1) + " days")
   

    input_domain = sys.argv[3]
    if input_domain not in ['large','small']:
        raise Exception("Invalid domain input entries. Current options: large, small. Case sensitive.")
         
else:
    raise Exception("Invalid input entries. Needs YYMMDD for start and end dates")



time_domains = ['60hr','84hr','120hr','180hr','day1','day2','day3','day4','day5','day6','day7']

time_labels = ['outlook hours 1-60','outlook hours 1-84','outlook hours 1-120','outlook hours 1-180',
               'day 1 outlook (hours 1-24)','day 2 outlook (hours 25-48)','day 3 outlook (hours 49-72)',
               'day 4 outlook (hours 73-96)','day 5 outlook (hours 97-120)','day 6 outlook (hours 121-144)',
               'day 7 outlook (hours 145-168)']

#stations = np.loadtxt(station_file,usecols=0,delimiter=',',dtype='str')

variables = ['PCPTOT', 'SFCWSPD_KF', 'SFCWSPD', 'PCPT6']
variable_names = ['Hourly Precipitation', 'Wind Speed-KF ', 'Wind Speed-Raw', '6-Hour Accumulated Precipitation']
variable_units = ['[mm/hr]','[km/hr]','[km/hr]', '[mm/6hr]']

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

        if "_" in model: #only for ENS models, which don't have grid res options
            legend_labels.append(model.replace("_"," "))
        else:
            legend_labels.append(model + grid)


#colors to plot, must be same length (or longer) than models list
model_colors = ['C0','C1','C2','C3','C4','C5','C6','C7','C8','C9','#ffc219','#CDB7F6','#65fe08','#fc3232','#754200','#00FFFF','#fc23ba','#a1a1a1','#000000','#000000','#000000','#000000']

# weighting curve steepness, Stull chose 100, so will test with that
k = 100

###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################



def get_rankings(variable,time_domain):
    
     POD_list,POFD_list,PSS_list, HSS_list, CSI_list, GSS_list, modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations = [],[],[],[],[],[],[],[], [], [], []
     
     leg_count = 0
     color_count = 0
    
     for i in range(len(models)):
        model = models[i] #loops through each model
        
        for grid in grids[i].split(","): #loops through each grid size for each model
        
        
            #ENS only has one grid (and its not saved in a g folder)
            if "ENS" in model:
                modelpath = model + '/'
                gridname = ""
            else:
                modelpath = model + '/' + grid + '/'
                gridname = "_" + grid
                        
            
            print("Now on.. " + model + gridname + "   " + variable)
            
            if os.path.isfile(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "CAT_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt"):
                #open the CAT file
                with open(textfile_folder +  modelpath  + input_domain + '/' + variable + '/' + "CAT_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt") as f:
                    CAT_lines = f.readlines()
        
                data_check = False
                #find the line for the given dates
                for CAT_line in CAT_lines:
                    if date_entry1 in CAT_line and date_entry2 in CAT_line:
                        POD = CAT_line.split("   ")[5]
                        POFD = CAT_line.split("   ")[6]
                        PSS = CAT_line.split("   ")[7]
                        HSS = CAT_line.split("   ")[8]
                        CSI = CAT_line.split("   ")[9]
                        GSS = CAT_line.split("   ")[10]
                        dataratio = CAT_line.split("   ")[11]
                        numstations = CAT_line.split("   ")[12].strip()
                        data_check = True


                if data_check == False:
                    print("   **Skipping " + model + grid + ", no data yet**")
                    skipped_modelnames.append(legend_labels[leg_count] + ":  (none)")
                    leg_count = leg_count+1
                    continue
                    
                #this removes models if more than half of data points are missing
                if int(dataratio.split("/")[0]) < int(dataratio.split("/")[1])/2: 
                    print("   **Skipping " + model + grid + ", less than 50% of data points**")
                    skipped_modelnames.append(legend_labels[leg_count] + ":  (" + dataratio + ")")
                    leg_count = leg_count+1
                    continue
                
                #only applies for the hrs and day 1 (not day2-7)
                if model + gridname in ["WRF3GEM_g3","WRF3GFS_g3","WRF3GFSgc01_g3","WRF4ICON_g3"]:
                    removed_hours = 3
                elif model + gridname in ["WRF3GEM_g4","WRF3GFS_g4"]:
                    removed_hours = 6
                elif model + gridname == "WRF3GFS_g5":
                    removed_hours = 9
                else:
                    removed_hours = 0
                
                # this checks how many stations were used in each average
                if int(numstations.split("/")[0]) < int(numstations.split("/")[1]): 
                    numofstations.append(legend_labels[leg_count] + ": (" + numstations + ")")
                
 
                POD_list.append(float(POD))
                POFD_list.append(float(POFD))
                PSS_list.append(float(PSS))
                HSS_list.append(float(HSS))
                CSI_list.append(float(CSI))
                GSS_list.append(float(GSS))
                modelcolors.append(model_colors[color_count])
                
                if int(dataratio.split("/")[0]) < int(dataratio.split("/")[1])-removed_hours*(delta+1):
                    if int(numstations.split("/")[0]) != int(numstations.split("/")[1]): 
                        modelnames.append(legend_labels[leg_count] + "*^")
                    else:
                        modelnames.append(legend_labels[leg_count] + "*")
                    edited_modelnames.append(legend_labels[leg_count] + ":  (" + dataratio + ")")
              
                else:
                    if int(numstations.split("/")[0]) != int(numstations.split("/")[1]): 
                        modelnames.append(legend_labels[leg_count] + "^")
                    else:
                        modelnames.append(legend_labels[leg_count])
                   
            #else:
            #    print("   Skipping  " + model + gridname + "   " + time_domain + " (doesn't exist)")
                

            leg_count = leg_count+1
         
        color_count = color_count+1
             
     return(POD_list,POFD_list,PSS_list, HSS_list, CSI_list, GSS_list, modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations)
 
def get_obs_dates(time_domain):
    
    def obs_days(add_start,add_end):
        obs_startdate = input_startdate + datetime.timedelta(days=add_start)
        obs_enddate = input_enddate + datetime.timedelta(days=add_end)       
        start = datetime.datetime.strftime(obs_startdate,"%b. %d, %Y")
        end = datetime.datetime.strftime(obs_enddate,"%b. %d, %Y")
        
        return(start + ' to ' + end)

    if time_domain == '60hr':
        obs_dates = obs_days(0,3)      
    elif time_domain == '84hr':
        obs_dates = obs_days(0,4)
    elif time_domain == '120hr':
        obs_dates = obs_days(0,5)       
    elif time_domain == '180hr':
        obs_dates = obs_days(0,8)      
    elif time_domain == 'day1':
        obs_dates = obs_days(0,0)
    elif time_domain == 'day2':
        obs_dates = obs_days(1,1)
    elif time_domain == 'day3':
        obs_dates = obs_days(2,2)
    elif time_domain == 'day4':
        obs_dates = obs_days(3,3)
    elif time_domain == 'day5':
        obs_dates = obs_days(4,4)
    elif time_domain == 'day6':
        obs_dates = obs_days(5,5)
    elif time_domain == 'day7':
        obs_dates = obs_days(6,6)        
    
    return(obs_dates)

def make_weights(var, time_domain, time_label,POD,POFD,PSS, HSS, CSI, GSS,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations):
    
    #sorts them greatest to least/least to greatest
    POD_sorted, modelnames_sortedPOD,modelcolors_sortedPOD = zip(*sorted(zip(POD, modelnames,modelcolors)))
    POFD_sorted, modelnames_sortedPOFD,modelcolors_sortedPOFD = zip(*sorted(zip(POFD, modelnames,modelcolors),reverse=True))
    PSS_sorted, modelnames_sortedPSS,modelcolors_sortedPSS = zip(*sorted(zip(PSS, modelnames,modelcolors)))
    HSS_sorted, modelnames_sortedHSS,modelcolors_sortedHSS = zip(*sorted(zip(HSS, modelnames,modelcolors)))
    CSI_sorted, modelnames_sortedCSI,modelcolors_sortedCSI = zip(*sorted(zip(CSI, modelnames,modelcolors)))
    GSS_sorted, modelnames_sortedGSS,modelcolors_sortedGSS = zip(*sorted(zip(GSS, modelnames,modelcolors)))

    POD_norm = preprocessing.normalize(POD_sorted)
    POFD_norm = preprocessing.normalize(POFD_sorted)
    PSS_norm = preprocessing.normalize(PSS_sorted)
    HSS_norm = preprocessing.normalize(HSS_sorted)
    CSI_norm = preprocessing.normalize(CSI_sorted)
    GSS_norm = preprocessing.normalize(GSS_sorted)

    POD_xo = np.median(POD_norm)
    POD_weight = 1/(1+exp(-k*(POD_norm-POD_xo)))

    POFD_xo = np.median(POFD_norm)
    POFD_weight = 1/(1+exp(-k*(POFD_norm-POFD_xo)))

    PSS_xo = np.median(PSS_norm)
    PSS_weight = 1/(1+exp(-k*(PSS_norm-PSS_xo)))

    HSS_xo = np.median(HSS_norm)
    HSS_weight = 1/(1+exp(-k*(HSS_norm-HSS_xo)))

    CSI_xo = np.median(CSI_norm)
    CSI_weight = 1/(1+exp(-k*(CSI_norm-CSI_xo)))

    GSS_xo = np.median(GSS_norm)
    GSS_weight = 1/(1+exp(-k*(GSS_norm-GSS_xo)))

    return(POD_weight, POFD_weight, PSS_weight, HSS_weight, CSI_weight, GSS_weight)

def main(args):
   # sys.stdout = open(logfilepath, "w") #opens log file
        
    var_i = 0
    for var in variables: #loop through variables
        
        time_count = 0
        for time_domain in time_domains:
            
            time_label = time_labels[time_count]
            
            if var == "PCPT24" and time_domain in ['60hr','84hr','120hr','180hr']:
                time_count = time_count+1
                continue
            
            #these returned variables are lists that contain one stat for each model (so length=#num of models)
            POD,POFD,PSS,HSS, CSI, GSS, edited_modelnames,skipped_modelnames,numofstations = get_rankings(var,time_domain)
            
            POD_weight, POFD_weight, PSS_weight, HSS_weight, CSI_weight, GSS_weight = make_weights(var, time_domain, time_label,POD,POFD,PSS, HSS, CSI, GSS,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations)

            weights_all = pd.DataFrame([POD_weight, POFD_weight, PSS_weight, HSS_weight, CSI_weight, GSS_weight], columns=edited_modelnames)
            weights_all.to_csv('weights_all')
            time_count = time_count+1
            


        var_i=var_i+1
            
    #sys.stdout.close() #close log file

if __name__ == "__main__":
    main(sys.argv)

