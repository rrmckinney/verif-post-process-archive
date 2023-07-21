start_date='211008'
end_date='211014'
i=((0))

while [ $((start_date)) -lt 230724 ]
do
	echo $start_date
	echo $end_date

	start_date=$(date -d $start_date"+7 days" +%y%m%d)
	end_date=$(date -d $end_date"+7 days" +%y%m%d)
	i=$((i))+7
done
