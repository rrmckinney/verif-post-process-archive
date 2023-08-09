#running on Sundays, this should always be for Sunday-Saturday (7 days)
  
source /home/verif/.bash_profile

#start_date=`date --date="-14 days" +%y%m%d`
#end_date=`date --date="-8 days" +%y%m%d`

conda activate verification

cd /home/verif/verif-post-process/src/

start_date='211001'
end_date='211007'

while [ $((start_date)) -lt 230731 ] 
do

        python3 leaderboards-txt-sqlite2.py $start_date $end_date PCPT6 small > log/lb_txt_PCPT6_sm.log
	python3 leaderboards-txt-sqlite2.py $start_date $end_date PCPT24 small > log/lb_txt_PCPT24_sm.log

        python3 leaderboards-txt-sqlite2.py $start_date $end_date PCPT6 large > log/lb_txt_PCPT6_lrg.log
        python3 leaderboards-txt-sqlite2.py $start_date $end_date PCPT24 large > log/lb_txt_PCPT24_lrg.log

	start_date=$(date -d $start_date"+7 days" +%y%m%d)
	end_date=$(date -d $end_date"+7 days" +%y%m%d)
done
