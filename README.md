This folder houses all the code that runs the verification and plotting for the https://weather.eos.ubc.ca/wxfcst/verification/

'''
BIN
'''

This folder is the executed code for the website stats and plots. Only the Bash scripts in this folder should be run
operationally. 

run-txt-weekly.sh : calculates the stats for each week for all variables.

run-txt-monthly.sh : calculates the stats for each month for all variables.

run-txt-precip-weekly.sh / run-txt-weekly-batch.sh : batch shell scrips to recalculate stats for missing dates. will be
					             will be depreciated after FALL 2023

run-plot-weekly.sh : runs the plots for each week and all variables. can only be run for a week AFTER the stats have been 
	             calculated

run-plot-monthly.sh : runs the plots for each month and all variables. can only be run for a month AFTER the stats have been
                     calculated

run-accum-error.sh : updates the accumulated error plot for each model on the website requires more than two weeks of data to run

run-meteograms.sh : creates meteograms for the UBC rooftop station for the different initialization times and variables.


'''
input
'''

This folder is all the input for the src files. It houses lists of the all the models and stations. As well as what 
variables and other characteristics each station holds.


'''
qc
'''

This is the quality check folder. Currenlty has a script that examines the distributions of the obs data at all stations.
This script was used to manually examine the data before implementing qc in the operational scripts. It is not used operationally


'''
src
'''

this is where all the source code run in bin is located.

leaderboards-txt-sqlite2.py : main code for running the stats (MAE, RMSE, correlation. relies heavily on utl/funcs.py for all its 			       functions. funcs2.py is used for other stats: SEEPS, PSS, contingency tables etc. 

leaderboards-plots.py : main code for running the plots of the stats. does not have a fucntion script

accum_error_alldays.py : main code for running the accumulated error. does not have a function script associated. 
