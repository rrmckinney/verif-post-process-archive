#!/usr/bin python

"""
Created in summer 2021

@author: evagnegy

This script is for the weekly leadership board 
 
"""

import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import pandas as pd
import warnings
import datetime

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

textfile_folder = '/verification/Statistics/'

###########################################################
### -------------------- INPUT ------------------------
###########################################################

# takes an input date for the last day of the week you want to include
if len(sys.argv) == 3:
    
    savetype = sys.argv[1]
    if savetype not in ['weekly','monthly']:
        raise Exception("Invalid time input entry. Current options: weekly, monthly. Case sensitive.")
         
    input_domain = sys.argv[2]
    if input_domain not in ['large','small']:
        raise Exception("Invalid domain input entry. Current options: large, small. Case sensitive.")
         
else:
    raise Exception("Invalid input entries. Needs time and input domain.")


time_domains = ['60hr','84hr','120hr','180hr','day1','day2','day3','day4','day5','day6','day7']

time_labels = ['outlook hours 1-60','outlook hours 1-84','outlook hours 1-120','outlook hours 1-180',
               'day 1 outlook (hours 1-24)','day 2 outlook (hours 25-48)','day 3 outlook (hours 49-72)',
               'day 4 outlook (hours 73-96)','day 5 outlook (hours 97-120)','day 6 outlook (hours 121-144)',
               'day 7 outlook (hours 145-168)']

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

date_test_file = textfile_folder + 'ENS/small/SFCTC/MAE_' + str(savetype) + '_SFCTC_60hr_' + input_domain + '.txt'

startdate = np.loadtxt(date_test_file,usecols=0,dtype='str')
enddate = np.loadtxt(date_test_file,usecols=1,dtype='str')

input_startdate = datetime.datetime.strptime(str(startdate), "%y%m%d").date()
print_startdate = datetime.datetime.strftime(input_startdate,"%m/%d/%y")

input_enddate = datetime.datetime.strptime(str(enddate), "%y%m%d").date()
print_enddate = datetime.datetime.strftime(input_enddate,"%m/%d/%y")
###########################################################
### -------------------- FUNCTIONS ------------------------
###########################################################



def get_rankings(variable,time_domain):

     leg_count = 0
     color_count = 0
    
     all_MAE_lists, all_RMSE_lists, all_corr_lists, modelnames,modelcolors = [],[],[],[],[]
     for i in range(len(models)):
        model = models[i] #loops through each model
        
        for grid in grids[i].split(","): #loops through each grid size for each model
        
        
            #ENS only has one grid (and its not saved in a g folder)
            if "ENS" in model:
                modelpath = model + '/' + input_domain +  '/' + variable + '/' 
            else:
                modelpath = model + '/' + grid + '/'+ input_domain +  '/' + variable + '/'
                
            MAE_file = textfile_folder +  modelpath + "MAE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt"
            print(MAE_file)           
            #skips time_domains that dont exist for this model
            if os.path.isfile(MAE_file):

                MAE_list = np.loadtxt(MAE_file,usecols=2,dtype=float)
    
                #counts lines in file
                with open(MAE_file, 'r') as fp:
                    for count, line in enumerate(fp):
                        pass

                if count+1 > 1:

                    RMSE_file = textfile_folder +  modelpath  + "RMSE_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt"
                    RMSE_list = np.loadtxt(RMSE_file,usecols=2,dtype=float)
                    
                    corr_file = textfile_folder +  modelpath + "spcorr_" + savetype + "_" + variable + "_" + time_domain + "_" + input_domain + ".txt"
                    corr_list = np.loadtxt(corr_file,usecols=2,dtype=float)
                      
                    # the ratios are the same for each statistic, so only checked once
                    dataratio = np.loadtxt(MAE_file,usecols=3,dtype=str)
    
                    expected = [i.split('/')[1] for i in dataratio]
                    actual = [i.split('/')[0] for i in dataratio]
                
                    #nans any value where less than half datapoints were there
                    MAE_list[:] = ["nan" if int(actual[x]) < int(expected[x])/2 else MAE_list[x] for x in range(len(MAE_list))] 
                    RMSE_list[:] = ["nan" if int(actual[x]) < int(expected[x])/2 else RMSE_list[x] for x in range(len(RMSE_list))] 
                    corr_list[:] = ["nan" if int(actual[x]) < int(expected[x])/2 else corr_list[x] for x in range(len(corr_list))] 
                         
                    all_MAE_lists.append(MAE_list)
                    all_RMSE_lists.append(RMSE_list)
                    all_corr_lists.append(corr_list)
                     
                    modelnames.append(legend_labels[leg_count])
                    modelcolors.append(model_colors[color_count])
                else:
                    print("Skipping " + modelpath + " (only one week available)")
                
            #else:
                #print("   Skipping  " + model + grid + "   " + time_domain + " (doesn't exist)")
               
            leg_count=leg_count+1
                
        color_count=color_count+1
        
     return(all_MAE_lists,all_RMSE_lists,all_corr_lists,modelnames,modelcolors)
 
def get_ranking_mean(MAE,RMSE,corr,modelnames,modelcolors):
    
    MAE_df = pd.DataFrame(MAE, index=modelnames)
    MAE_rank = MAE_df.rank(method='min')
    MAE_rank_avgs = MAE_rank.mean(axis=1)

    weeks_in_avg = MAE_df.shape[1] - pd.isnull(MAE_df).sum(axis=1)
    MAE_avg = pd.concat([MAE_rank_avgs, weeks_in_avg], axis=1)
    MAE_avg['colors'] = modelcolors

    RMSE_df = pd.DataFrame(RMSE, index=modelnames)
    RMSE_rank = RMSE_df.rank(method='min')
    RMSE_rank_avgs = RMSE_rank.mean(axis=1)

    weeks_in_avg = RMSE_df.shape[1] - pd.isnull(RMSE_df).sum(axis=1)
    RMSE_avg = pd.concat([RMSE_rank_avgs, weeks_in_avg], axis=1)
    RMSE_avg['colors'] = modelcolors
    
    corr_df = pd.DataFrame(corr, index=modelnames)
    corr_rank = corr_df.rank(method='min',ascending=False)
    corr_rank_avgs = corr_rank.mean(axis=1)

    weeks_in_avg = corr_df.shape[1] - pd.isnull(corr_df).sum(axis=1)
    corr_avg = pd.concat([corr_rank_avgs, weeks_in_avg], axis=1)
    corr_avg['colors'] = modelcolors
    
    num_weeks = RMSE_df.shape[1]
    
    edited_modelnames_all,edited_modelnames = [],[]
    missing_weeks = False
    for i in range(MAE_df.shape[0]):
        if MAE_avg[1][i] < num_weeks:
            edited_modelnames_all.append(MAE_avg.index[i] + "*")
            edited_modelnames.append(MAE_avg.index[i] + ":  (" + str(MAE_avg[1][i]) + "/" + str(num_weeks) + ")")
            missing_weeks = True
        else:
            edited_modelnames_all.append(MAE_avg.index[i])
    
    MAE_avg['edited_names'] = edited_modelnames_all
    RMSE_avg['edited_names'] = edited_modelnames_all
    corr_avg['edited_names'] = edited_modelnames_all
    
    return(MAE_avg,RMSE_avg,corr_avg,num_weeks,missing_weeks,edited_modelnames)

def get_ranking_med(MAE,RMSE,corr,modelnames,modelcolors):
    
    MAE_df = pd.DataFrame(MAE, index=modelnames)
    MAE_rank = MAE_df.rank(method='min')
    MAE_rank_meds = MAE_rank.median(axis=1)

    weeks_in_med = MAE_df.shape[1] - pd.isnull(MAE_df).sum(axis=1)
    MAE_med = pd.concat([MAE_rank_meds, weeks_in_med], axis=1)
    MAE_med['colors'] = modelcolors

    RMSE_df = pd.DataFrame(RMSE, index=modelnames)
    RMSE_rank = RMSE_df.rank(method='min')
    RMSE_rank_meds = RMSE_rank.median(axis=1)

    weeks_in_med = RMSE_df.shape[1] - pd.isnull(RMSE_df).sum(axis=1)
    RMSE_med = pd.concat([RMSE_rank_meds, weeks_in_med], axis=1)
    RMSE_med['colors'] = modelcolors
    
    corr_df = pd.DataFrame(corr, index=modelnames)
    corr_rank = corr_df.rank(method='min',ascending=False)
    corr_rank_meds = corr_rank.median(axis=1)

    weeks_in_med = corr_df.shape[1] - pd.isnull(corr_df).sum(axis=1)
    corr_med = pd.concat([corr_rank_meds, weeks_in_med], axis=1)
    corr_med['colors'] = modelcolors
    
    num_weeks = RMSE_df.shape[1]
    
    edited_modelnames_all,edited_modelnames = [],[]
    missing_weeks = False
    for i in range(MAE_df.shape[0]):
        if MAE_med[1][i] < num_weeks:
            edited_modelnames_all.append(MAE_med.index[i] + "*")
            edited_modelnames.append(MAE_med.index[i] + ":  (" + str(MAE_med[1][i]) + "/" + str(num_weeks) + ")")
            missing_weeks = True
        else:
            edited_modelnames_all.append(MAE_med.index[i])
    
    MAE_med['edited_names'] = edited_modelnames_all
    RMSE_med['edited_names'] = edited_modelnames_all
    corr_med['edited_names'] = edited_modelnames_all
    
    return(MAE_med,RMSE_med,corr_med,num_weeks,missing_weeks,edited_modelnames)


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

def make_leaderboard_sorted(var, var_name, var_unit, time_domain, time_label,MAE,RMSE,corr,num_weeks,missing_weeks,edited_modelnames,modelnames,stat_type):     
    print(MAE)
    MAE_sorted, modelnames_MAE, colors_MAE = zip(*sorted(zip(MAE[0], MAE['edited_names'],MAE['colors']),reverse=True))
    RMSE_sorted, modelnames_RMSE, colors_RMSE = zip(*sorted(zip(RMSE[0], RMSE['edited_names'], RMSE['colors']),reverse=True))
    corr_sorted, modelnames_corr, colors_corr = zip(*sorted(zip(corr[0], corr['edited_names'], corr['colors']),reverse=True))
   
      
    #plotting
    x = np.arange(len(modelnames))
    width = 0.6
       
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3,figsize=(25,17),dpi=150)
    plt.tight_layout(w_pad=20)
    plt.subplots_adjust(top=0.9)
    
    obs_dates = get_obs_dates(time_domain)

    fig.suptitle(var_name + ' ' + savetype + ' ' + stat_type + ' rankings from ' + str(obs_dates) + " for " + time_label + "  [model init dates: " + str(print_startdate) + '-' + str(print_enddate) + " (" + str(num_weeks) + " " + savetype[:-2] + "s)]" ,fontsize=25)
    
    ax1.barh(x, MAE_sorted, width,color=colors_MAE,edgecolor='k',linewidth=2.5)
    ax1.set_yticks(x)
    ax1.set_yticklabels(modelnames_MAE,fontsize=18)
    ax1.set_title("Mean Absolute Error (MAE) Ranking",fontsize=18)
    ax1.set_xlabel(var_name + " " + stat_type + " " + savetype + " ranking",fontsize=20)
    
    ax2.barh(x, RMSE_sorted, width,color=colors_RMSE,edgecolor='k',linewidth=2.5)
    ax2.set_yticks(x)
    ax2.set_yticklabels(modelnames_RMSE,fontsize=18)
    ax2.set_title("Root Mean Square Error (RMSE) Ranking",fontsize=18)
    ax2.set_xlabel(var_name + " " + stat_type + " " + savetype + " ranking",fontsize=20)
    
    ax3.barh(x, corr_sorted, width,color=colors_corr,edgecolor='k',linewidth=2.5)
    ax3.set_yticks(x)
    ax3.set_yticklabels(modelnames_corr,fontsize=18)
    ax3.set_title("Spearman Correlation Ranking",fontsize=18)
    ax3.set_xlabel(var_name + " "  + stat_type +  " " + savetype + " ranking",fontsize=20)
    
    
    for ax in [ax1, ax2, ax3]:
        ax.tick_params(axis='x', labelsize=20)
        ax.set_ylim(-0.9,len(modelnames)-width*0.5)
        ax.set_axisbelow(True)
        ax.grid(True,axis='x')
      
     
    if missing_weeks == True:   
        if len(modelnames) > 50:
            y=-135
            x=-5
            div=1
        elif len(modelnames) > 40:
            y=-125
            x=-5
            div=1
        elif len(modelnames) > 20:
            y=-60
            x=-3
            div=2
        else:
            y=-48
            x=-2
            div=2
        plt.text(y, x, "*Total weeks in calculation:", fontsize=18)
        for i in range(len(edited_modelnames)):
            plt.text(y, x-(1/div) - (i/div), "            " + edited_modelnames[i], fontsize=18)
    
    plt.savefig(save_folder + '/rankings/' + input_domain + '_' + var + '_' + savetype + '_' + time_domain + '_' + stat_type + '.png',bbox_inches='tight')

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
            MAE,RMSE,corr,modelnames,modelcolors = get_rankings(var,time_domain)
            
            MAE_avg,RMSE_avg,corr_avg,num_weeks_avg,missing_weeks_avg,edited_modelnames_avg = get_ranking_mean(MAE,RMSE,corr,modelnames,modelcolors)
            MAE_med,RMSE_med,corr_med,num_weeks_med,missing_weeks_med,edited_modelnames_med = get_ranking_med(MAE,RMSE,corr,modelnames,modelcolors)
            
            make_leaderboard_sorted(var, variable_names[var_i], variable_units[var_i], time_domain,time_label, MAE_avg,RMSE_avg,corr_avg,num_weeks_avg,missing_weeks_avg,edited_modelnames_avg, modelnames,"average")
            make_leaderboard_sorted(var, variable_names[var_i], variable_units[var_i], time_domain,time_label, MAE_med,RMSE_med,corr_med,num_weeks_med,missing_weeks_med,edited_modelnames_med, modelnames,"median")

    
            time_count = time_count+1

        var_i=var_i+1
            
    #sys.stdout.close() #close log file

if __name__ == "__main__":
    main(sys.argv)

