#!/usr/bin python

"""
Created in summer 2021

@author: evagnegy

This script is for the weekly leadership board 
 
"""

import matplotlib.pyplot as plt
import os
import numpy as np
import datetime #import datetime, timedelta
import sys


import warnings
warnings.filterwarnings("ignore",category=RuntimeWarning)
###########################################################
### -------------------- FILEPATHS ------------------------
###########################################################


#path to save the log/output
#logfilepath = "/home/egnegy/python_plotting/log/plot_leaderboards.log"

#location to save the images
save_folder = '/www_oper/results/verification/images/leaderboards/'

#description file for stations
#station_file = '/home/egnegy/ensemble-verification/testing_stations/input/station_list_leaderboards.txt'

#description file for models
models_file = '/home/verif/verif-get-data/input/model_list.txt'

textfile_folder = '/scratch/verif/verification/statistics/'

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

variables = ['SFCTC_KF','SFCTC','PCPTOT', 'SFCWSPD_KF', 'SFCWSPD', 'APCP6','APCP24']
variable_names = ['Temperature-KF', 'Temperature-Raw','Hourly Precipitation', 'Wind Speed-KF ', 'Wind Speed-Raw', '6-Hour Accumulated Precipitation', '24-Hour Accumulated Precipitation']
variable_units = ['[C]','[C]', '[mm/hr]','[km/hr]','[km/hr]', '[mm/6hr]','[mm/day]']



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


###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################



def get_rankings(variable,time_domain):
    
     MAE_list,RMSE_list,correlation_list,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations = [],[],[],[],[],[],[],[]
     
     leg_count = 0
     color_count = 0
    
     for i in range(len(models)):
        model = models[i] #loops through each model
        
        for grid in grids[i].split(","): #loops through each grid size for each model
        
        
            #ENS only has one grid (and its not saved in a g folder)
            if "ENS" in model:
                modelpath = model + '/'+ input_domain + '/' + savetype + '/' + variable + '/'
                gridname = ""
            else:
                modelpath = model + '/' + grid + '/'+ input_domain + '/' + savetype + '/' + variable + '/'
                gridname = "_" + grid
                        
            
            print("Now on.. " + model + gridname + "   " + variable)
            
            if os.path.isfile(textfile_folder +  modelpath + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt"):
                #open the MAE file
                with open(textfile_folder +  modelpath + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt") as f:
                    MAE_lines = f.readlines()
        
                data_check = False
                #find the line for the given dates
                
                for MAE_line in MAE_lines:
                    if date_entry1 in MAE_line and date_entry2 in MAE_line:
                        MAE = MAE_line.split("   ")[1]
                        dataratio = MAE_line.split("   ")[2]
                        numstations = MAE_line.split("   ")[3].strip()
                        data_check = True


                if data_check == False:
                    print("   **Skipping " + model + grid + ", no data yet**")
                    skipped_modelnames.append(legend_labels[leg_count] + ":  (none)")
                    leg_count = leg_count+1
                    continue
                    
                    
                #open the RMSE file
                with open(textfile_folder +  modelpath + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt") as f:
                    RMSE_lines = f.readlines()
        
                #find the line for the given dates
                for RMSE_line in RMSE_lines:
                    if date_entry1 in RMSE_line and date_entry2 in RMSE_line:
                        RMSE = RMSE_line.split("   ")[1]
                  
                #open the MAE file
                with open(textfile_folder +  modelpath + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt") as f:
                    spcorr_lines = f.readlines()
        
                #find the line for the given dates
                for spcorr_line in spcorr_lines:
                    if date_entry1 in spcorr_line and date_entry2 in spcorr_line:
                        spcorr = spcorr_line.split("   ")[1]
        
            
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
                
 
                MAE_list.append(float(MAE))
                RMSE_list.append(float(RMSE))
                correlation_list.append(float(spcorr))
                modelcolors.append(model_colors[color_count])
                
                print(legend_labels[leg_count])
                print(dataratio)
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
             
     
     return(MAE_list,RMSE_list,correlation_list,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations)
 
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


def make_leaderboard_sorted(var, var_name, var_unit, time_domain, time_label,MAE,RMSE,corr,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations):     


    #sorts them greatest to least/least to greatest
    MAE_sorted, modelnames_sortedMAE,modelcolors_sortedMAE = zip(*sorted(zip(MAE, modelnames,modelcolors),reverse=True))
    RMSE_sorted, modelnames_sortedRMSE,modelcolors_sortedRMSE = zip(*sorted(zip(RMSE, modelnames,modelcolors),reverse=True))
    corr_sorted, modelnames_sortedcorr,modelcolors_sortedcorr = zip(*sorted(zip(corr, modelnames,modelcolors)))
    
    #plotting
    x = np.arange(len(modelnames))
    width = 0.6
       
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3,figsize=(25,17),dpi=150)
    plt.tight_layout(w_pad=20)
    plt.subplots_adjust(top=0.9)
    
    obs_dates = get_obs_dates(time_domain)
    
    fig.suptitle(var_name + ' ' + savetype + ' stats from ' + str(obs_dates) + " for " + time_label + "  [model init. dates: " + str(print_startdate) + '-' + str(print_enddate) + " (" + str(delta+1) + " days)]",fontsize=25)
    
    ax1.barh(x, MAE_sorted, width,color=modelcolors_sortedMAE,edgecolor='k',linewidth=2.5)
    ax1.set_yticks(x)
    ax1.set_yticklabels(modelnames_sortedMAE,fontsize=18)
    ax1.set_title("Mean Absolute Error (MAE)",fontsize=18)
    ax1.set_xlabel(var_name + " MAE " + var_unit,fontsize=20)
    
    ax2.barh(x, RMSE_sorted, width,color=modelcolors_sortedRMSE,edgecolor='k',linewidth=2.5)
    ax2.set_yticks(x)
    ax2.set_yticklabels(modelnames_sortedRMSE,fontsize=18)
    ax2.set_title("Root Mean Square Error (RMSE)",fontsize=18)
    ax2.set_xlabel(var_name + " RMSE " + var_unit,fontsize=20)
    
    left_lim=0
    
    if any(np.array(corr_sorted)<0):
        left_lim = np.nanmin(corr_sorted) - 0.05

        
    ax3.barh(x, corr_sorted, width,color=modelcolors_sortedcorr,edgecolor='k',linewidth=2.5)
    ax3.set_yticks(x)
    ax3.set_yticklabels(modelnames_sortedcorr,fontsize=18)
    ax3.set_title("Spearman Correlation",fontsize=18)
    ax3.set_xlim(left_lim,1)
    ax3.set_xlabel(var_name + " Spearman Correlation",fontsize=20)
    
    
    for ax in [ax1, ax2, ax3]:
        ax.tick_params(axis='x', labelsize=20)
        ax.set_ylim(-0.9,len(modelnames)-width*0.5)
        ax.set_axisbelow(True)
        ax.grid(True,axis='x')
      
    if edited_modelnames != []:    
        plt.text(-3, -4, "*Missing model data:", fontsize=18)
        for x in range(len(edited_modelnames)):
            plt.text(-3, -5 -x, "            " + edited_modelnames[x], fontsize=18)
    
    if numofstations != []:  
        plt.text(-2, -4, "^Number of stations averaged:", fontsize=18)
        
        if len(numofstations) > 25:
           for x in range(25):
                plt.text(-2, -5 -x, "            " + numofstations[x], fontsize=18) 
           for x in range(len(numofstations)-25):
                plt.text(-1.3, -5 -x, "            " + numofstations[x+25], fontsize=18)
        else:
            for x in range(len(numofstations)):
                plt.text(-2, -5 -x, "            " + numofstations[x], fontsize=18)
        
            
    if skipped_modelnames != []:    
        plt.text(0, -4, "Skipped models (<50% datapoints):", fontsize=18)
        for x in range(len(skipped_modelnames)):
            plt.text(0, -5 -x, "            " + skipped_modelnames[x], fontsize=18)
    

    plt.savefig(save_folder + '/best_to_worst/' + input_domain + '_' + var + '_' + savetype + '_' + time_domain + '.png',bbox_inches='tight')
    

def make_leaderboard_unsorted(var, var_name, var_unit, time_domain):     

    #these returned variables are lists that contain one stat for each model (so length=#num of models)
    MAE,RMSE,corr,modelnames,modelcolors,edited_modelnames,skipped_modelnames,missingstationdata,numofstations = get_rankings(var, time_domain)
    
    #plotting
    x = np.arange(len(modelnames))
    width = 0.6
       
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3,figsize=(25,17),dpi=150)
    plt.tight_layout(w_pad=20)
    plt.subplots_adjust(top=0.9)
    
    obs_dates = get_obs_dates(time_domain)
    
    fig.suptitle(var_name + ' ' + savetype + ' stats for ' + obs_dates + " (" + str(delta+1) + " days) for " + time_domain,fontsize=20)
    
    ax1.barh(x, MAE, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax1.set_yticks(x)
    ax1.set_yticklabels(modelnames,fontsize=18)
    ax1.set_title("Mean Absolute Error (MAE)",fontsize=18)
    ax1.set_xlabel(var_name + " MAE " + var_unit,fontsize=20)
    
    ax2.barh(x, RMSE, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax2.set_yticks(x)
    ax2.set_yticklabels(modelnames,fontsize=18)
    ax2.set_title("Root Mean Square Error (RMSE)",fontsize=18)
    ax2.set_xlabel(var_name + " RMSE " + var_unit,fontsize=20)
    
    ax3.barh(x, corr, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax3.set_yticks(x)
    ax3.set_yticklabels(modelnames,fontsize=18)
    ax3.set_title("Spearman Correlation",fontsize=18)
    ax3.set_xlim(0,1)
    ax3.set_xlabel(var_name + " Spearman Correlation",fontsize=20)
    
    
    for ax in [ax1, ax2, ax3]:
        ax.tick_params(axis='x', labelsize=20)
        ax.set_ylim(-0.9,len(modelnames)-width*0.5)
        ax.set_axisbelow(True)
        ax.grid(True,axis='x')
      
        
    plt.text(-3, -4, "*Missing model data:", fontsize=18)
    for x in range(len(edited_modelnames)):
        plt.text(-3, -5 -x, "            " + edited_modelnames[x], fontsize=18)
    
    plt.text(-2, -4, "^Number of stations averaged:", fontsize=18)
    for x in range(len(numofstations)):
        plt.text(-2, -5 -x, "            " + numofstations[x], fontsize=18)

    if skipped_modelnames != []:    
        plt.text(0, -4, "Skipped models (<50% datapoints):", fontsize=18)
        for x in range(len(skipped_modelnames)):
            plt.text(0, -5 -x, "            " + skipped_modelnames[x], fontsize=18)
    

    plt.savefig(save_folder + input_domain + '_' + var + '_' + savetype + '_unsorted_' + time_domain + '.png',bbox_inches='tight')

def make_leaderboard_gridsize(var, var_name, var_unit, time_domain):     

    #these returned variables are lists that contain one stat for each model (so length=#num of models)
    MAE,RMSE,corr,modelnames,modelcolors_unused,edited_modelnames,skipped_modelnames,missingstationdata,numofstations = get_rankings(var, time_domain)
    
    #gave value of 1000 to MPASg1 MPASg2 and SREF so theyre at the top (unknown grids)
    grids = [36,12,4,36,12,4,1.3,36,12,4,36,12,4,36,12,4,1.3,36,12,4,27,9,27,9,27,9,36,12,4,36,12,108,36,12,81,27,9,108,36,12,81,27,9,1000,1001,1002]
    
    
    #sorts them greatest to least/least to greatest
    gridsorted, MAE_sorted, modelnames_sortedMAE = zip(*sorted(zip(grids,MAE, modelnames)))
    gridsorted, RMSE_sorted, modelnames_sortedRMSE = zip(*sorted(zip(grids,RMSE, modelnames)))
    gridsorted, corr_sorted, modelnames_sortedcorr = zip(*sorted(zip(grids,corr, modelnames)))
    
    modelcolors = np.hstack(([['#000000'],['#fc23ba']*2,['C0']*2,['C1']*2,['C2']*10,['C3']*5,['C4']*10,['C5']*5,['C6']*7,['C7']*2]))
    modelcolors = modelcolors[::-1]
    
    #plotting
    x = np.arange(len(modelnames))
    width = 0.6
       
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3,figsize=(25,17),dpi=150)
    plt.tight_layout(w_pad=20)
    plt.subplots_adjust(top=0.9)
    
    
    fig.suptitle(var_name + ' ' + savetype + ' stats from ' + start_date + ' to ' + end_date + " (" + str(delta+1) + " days) for " + time_domain,fontsize=20)
    
    ax1.barh(x, MAE_sorted, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax1.set_yticks(x)
    ax1.set_yticklabels(modelnames_sortedMAE,fontsize=18)
    ax1.set_title("Mean Absolute Error (MAE)",fontsize=18)
    ax1.set_xlabel(var_name + " MAE " + var_unit,fontsize=20)
    
    ax2.barh(x, RMSE_sorted, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax2.set_yticks(x)
    ax2.set_yticklabels(modelnames_sortedRMSE,fontsize=18)
    ax2.set_title("Root Mean Square Error (RMSE)",fontsize=18)
    ax2.set_xlabel(var_name + " RMSE " + var_unit,fontsize=20)
    
    ax3.barh(x, corr_sorted, width,color=modelcolors,edgecolor='k',linewidth=2.5)
    ax3.set_yticks(x)
    ax3.set_yticklabels(modelnames_sortedcorr,fontsize=18)
    ax3.set_title("Spearman Correlation",fontsize=18)
    ax3.set_xlim(0,1)
    ax3.set_xlabel(var_name + " Spearman Correlation",fontsize=20)
    
    
    for ax in [ax1, ax2, ax3]:
        ax.tick_params(axis='x', labelsize=20)
        ax.set_ylim(-0.9,len(modelnames)-width*0.5)
        ax.set_axisbelow(True)
        ax.grid(True,axis='x')
      
        
    plt.text(-3, -4, "*Missing model data:", fontsize=18)
    for x in range(len(edited_modelnames)):
        plt.text(-3, -5 -x, "            " + edited_modelnames[x], fontsize=18)
    
    plt.text(-2, -4, "^Number of stations averaged:", fontsize=18)
    
    
    if len(numofstations) > 22:
       for x in range(22):
            plt.text(-2, -5 -x, "            " + numofstations[x], fontsize=18) 
       for x in range(len(numofstations)-22):
            plt.text(-2.5, -5 -x, "            " + numofstations[x+22], fontsize=18)
    else:
        for x in range(len(numofstations)):
            plt.text(-2, -5 -x, "            " + numofstations[x], fontsize=18)
        

    if skipped_modelnames != []:    
        plt.text(0, -4, "Skipped models (<50% datapoints):", fontsize=18)
        for x in range(len(skipped_modelnames)):
            plt.text(0, -5 -x, "            " + skipped_modelnames[x], fontsize=18)
    

    plt.savefig(save_folder + input_domain + '_' + var + '_' + savetype + '_gridsize_'+time_domain+'.png',bbox_inches='tight')
    
def main(args):
   # sys.stdout = open(logfilepath, "w") #opens log file
        
    var_i = 0
    for var in variables: #loop through variables
        
        time_count = 0
        for time_domain in time_domains:
            
            time_label = time_labels[time_count]
            
            if var == "APCP24" and time_domain in ['60hr','84hr','120hr','180hr']:
                time_count = time_count+1
                continue
            
            #these returned variables are lists that contain one stat for each model (so length=#num of models)
            MAE,RMSE,corr,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations = get_rankings(var,time_domain)
            
            print(var)
            print(MAE)
            print(modelnames)
            
            make_leaderboard_sorted(var, variable_names[var_i], variable_units[var_i], time_domain,time_label, MAE,RMSE,corr,modelnames,modelcolors,edited_modelnames,skipped_modelnames,numofstations)
           # make_leaderboard_unsorted(var, variable_names[var_i], variable_units[var_i], time_domain)
           # make_leaderboard_gridsize(var, variable_names[var_i], variable_units[var_i], time_domain)
           
            time_count = time_count+1

        var_i=var_i+1
            
    #sys.stdout.close() #close log file

if __name__ == "__main__":
    main(sys.argv)

