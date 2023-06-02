#running on Mondays, this should always be for Monday-Sunday (7 days)

source /home/verif/.bash_profile

start_date=`date --date="-14 days" +%y%m%d`
end_date=`date --date="-8 days" +%y%m%d`

conda activate verification

cd /home/verif/verif-post-process/src/

python3 leaderboard-txt.py $start_date $end_date SFCTC small > log/lb_txt_SFCTC_sm.log
python3 leaderboard-txt.py $start_date $end_date SFCTC_KF small > log/lb_txt_SFCTC_KF_sm.log
python3 leaderboard-txt.py $start_date $end_date SFCWSPD small > log/lb_txt_SFCWSPD_sm.log
python3 leaderboard-txt.py $start_date $end_date SFCWSPD_KF small > log/lb_txt_SFCWSPD_KF_sm.log
python3 leaderboard-txt.py $start_date $end_date PCPTOT small > log/lb_txt_PCPTOT_sm.log
python3 leaderboard-txt.py $start_date $end_date APCP6 small > log/lb_txt_APCP6_s.log
python3 leaderboard-txt.py $start_date $end_date APCP24 small > log/lb_txt_APCP24_sm.log

python3 leaderboard-txt.py $start_date $end_date SFCTC large > log/lb_txt_SFCTC_lrg.log
python3 leaderboard-txt.py $start_date $end_date SFCTC_KF large > log/lb_txt_SFCTC_KF_lrg.log
python3 leaderboard-txt.py $start_date $end_date SFCWSPD large > log/lb_txt_SFCWSPD_lrg.log
python3 leaderboard-txt.py $start_date $end_date SFCWSPD_KF large > log/lb_txt_SFCWSPD_KF_lrg.log
python3 leaderboard-txt.py $start_date $end_date PCPTOT large > log/lb_txt_PCPTOT_lrg.log
python3 leaderboard-txt.py $start_date $end_date APCP6 large > log/lb_txt_APCP6_lrg.log
python3 leaderboard-txt.py $start_date $end_date APCP24 large > log/lb_txt_APCP24_lrg.log

