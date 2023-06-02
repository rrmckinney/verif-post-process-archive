source /home/verif/.bash_profile

start_date=`date --date="-15 days" +%y%m%d`
end_date=`date --date="-9 days" +%y%m%d`

conda activate verification

cd /home/verif/verif-post-process/src/

python3 leaderboards-plots.py $start_date $end_date small > log/lb_plots_sm.log
python3 leaderboards-plots.py $start_date $end_date large > log/lb_plots_lrg.log
python3 leaderboard-avg-ranking.py weekly small > log/lb_avg_ranking_sm.log
python3 leaderboard-avg-ranking.py weekly large > log/lb_avg_ranking_lrg.log

