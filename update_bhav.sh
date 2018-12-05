rm PR*.zip 2> /dev/null
rm fo_mktlots.csv 2> /dev/null
FILE=PR051218.zip
wget -U \"Mozilla/5.0\" -c https://www.nseindia.com/archives/equities/bhavcopy/pr/$FILE --no-check-certificate
wget -U \"Mozilla/5.0\" -c https://www.nseindia.com/content/fo/fo_mktlots.csv --no-check-certificate

if [ -f fo_mktlots.csv ]; then
sed -i 's/ //g' fo_mktlots.csv
fi

if [ -f $FILE ]; then
mkdir -p pr_folder
unzip PR*.zip -d pr_folder
echo "delete from \"BHAV_COPY_PD\";" > add_bhav.sql
echo "delete from Corporate_Actions_BC;" >> add_bhav.sql
echo ".mode csv" >> add_bhav.sql
pd_file=`ls pr_folder/Pd*.csv`
echo ".import $pd_file \"BHAV_COPY_PD\"" >> add_bhav.sql
echo "delete from BHAV_COPY_PD WHERE MKT=='MKT';" >> add_bhav.sql
echo "delete from BHAV_COPY_PD WHERE NET_TRDVAL==\" \";" >> add_bhav.sql
bc_file=`ls pr_folder/Bc*.csv`
echo ".import $bc_file \"Corporate_Actions_BC\"" >> add_bhav.sql
echo "delete from Corporate_Actions_BC WHERE SERIES=='SERIES'" >> add_bhav.sql
sqlite3 DAILY_BHAV_PR < add_bhav.sql
rm -rf pr_folder
rm PR*.zip
fi
