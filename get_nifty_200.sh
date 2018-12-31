wget -U "Mozilla/5.0" https://in.investing.com/indices/cnx-200-components --no-check-certificate -O nifty_200.html
python in.investing.py
perl -i.bak -pwe 'tr/\0//d' nifty_200.csv
