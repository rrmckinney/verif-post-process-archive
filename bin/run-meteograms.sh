#!/bin/bash -l
#plot_date=`date --date="-7 days" +%y%m%d`
plot_date='230701'
# must be -8 if its after 0 server time, -7 if 22 or 23 server time
plot_date_ibcs=`date --date="-9 days" +%y%m%d`

conda activate verification

#python3 /home/verif/verif-post-process/src/ibcs_meteograms.py $plot_date_ibcs
#mailx -s "ibcs_meteograms log $plot_date_ibcs" rmckinney@eoas.ubc.ca < /home/verif/verif-post-process/log/ibcs_meteogram.log

python3 /home/verif/verif-post-process/src/meteograms.py $plot_date
#mailx -s "meteograms log $plot_date" rmckinney < /home/verif/verif-post-process/src/log/meteograms.log

