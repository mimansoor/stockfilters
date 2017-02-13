#/usr/bin/perl

use lib qw( ..);
use JSON;
use LWP::Simple;
use DBI;
use DateTime qw(:all);

sub check_for_download_time {
	my $time_now = DateTime->now( time_zone => 'local' );
	my $download = 1;

	#Start only after 9:15AM
	#Stop after 15:30PM
	if ($time_now->hour < 9 || $time_now->hour > 15) {
		$download = 0;
	} else {
		if ($time_now->hour == 9) {
			if ($time_now->minute < 15) {
				$download = 0;
			}
		} else {
			if ($time_now->hour == 15) {
				if ($time_now->minute > 30) {
					$download = 0;
				}
			}
		}
	}

	return $download;
}

my $url = "http://www.google.com/finance/info?infotype=infoquoteall&q=NSE:NIFTY, NSE:GRASIM, NSE:TECHM, NSE:TCS, NSE:INFY, NSE:ADANIPORTS, NSE:BANKBARODA, NSE:INDUSINDBK, NSE:TATASTEEL, NSE:NTPC, NSE:KOTAKBANK, NSE:HCLTECH, NSE:COALINDIA, NSE:HDFCBANK, NSE:AXISBANK, NSE:TATAPOWER, NSE:EICHERMOT, NSE:LT, NSE:BAJAJ-AUTO, NSE:ONGC, NSE:BHARTIARTL, NSE:BHEL, NSE:YESBANK, NSE:WIPRO, NSE:ACC, NSE:SUNPHARMA, NSE:ULTRACEMCO, NSE:HEROMOTOCO, NSE:HINDUNILVR, NSE:AMBUJACEM, NSE:SBIN, NSE:RELIANCE, NSE:IDEA, NSE:HDFC, NSE:TATAMTRDVR, NSE:HINDALCO, NSE:MARUTI, NSE:TATAMOTORS, NSE:INFRATEL, NSE:ICICIBANK, NSE:ZEEL, NSE:ASIANPAINT, NSE:BOSCHLTD, NSE:POWERGRID, NSE:ITC, NSE:CIPLA, NSE:GAIL, NSE:DRREDDY, NSE:LUPIN, NSE:BPCL, NSE:AUROPHARMA, NSE:NIFTYJR, NSE:MCDOWELL-N, NSE:BAJAJFINSV, NSE:ABB, NSE:MOTHERSUMI, NSE:BAJFINANCE, NSE:SRTRANSFIN, NSE:CONCOR, NSE:PIDILITIND, NSE:OFSS, NSE:COLPAL, NSE:TITAN, NSE:MARICO, NSE:TORNTPHARM, NSE:OIL, NSE:GLAXO, NSE:BRITANNIA, NSE:PNB, NSE:EMAMILTD, NSE:DLF, NSE:PEL, NSE:SHREECEM, NSE:CASTROLIND, NSE:HAVELLS, NSE:GODREJCP, NSE:PFC, NSE:IBULHSGFIN, NSE:GSKCONS, NSE:DABUR, NSE:GLENMARK, NSE:JSWSTEEL, NSE:VEDL, NSE:LICHSGFIN, NSE:CADILAHC, NSE:BHARATFORG, NSE:PGHH, NSE:SIEMENS, NSE:APOLLOHOSP, NSE:HINDPETRO, NSE:DIVISLAB, NSE:INDIGO, NSE:BEL, NSE:NMDC, NSE:UPL, NSE:UBL, NSE:CUMMINSIND, NSE:HINDZINC, NSE:IOC, NSE:BANKNIFTY";


system("cp realtime_data.db tmp_abcdefgh.db");

my $driver   = "SQLite";
my $database = "tmp_abcdefgh.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";

$id = 101;

while (1) {
	if (check_for_download_time()) {
		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;
		my $json_str = get($url);
		die "Couldn't get $url" unless defined $json_str;

		$json_str =~ s/\/\///;

		my $data;

		$data = decode_json( $json_str );
		foreach my $item (@{$data}) {
			#print "$item->{'ltt'},$item->{'op'},$item->{'hi'},$item->{'lo'},$item->{'l'},$item->{'vo'}\n";
			undef $stmt;
			my $name = $item->{'t'};
			$name =~ s/,//;
			my $open = $item->{'op'};
			$open =~ s/,//;
			my $high = $item->{'hi'};
			$high =~ s/,//;
			my $low  = $item->{'lo'};
			$low =~ s/,//;
			my $last = $item->{'l_fix'};
			$last =~ s/,//;
			my $prev_close = $item->{'pcls_fix'};
			$prev_close =~ s/,//;
			my $change = $item->{'c_fix'};
			$change =~ s/,//;
			my $change_per = $item->{'cp_fix'};
			$change_per =~ s/,//;
			my $volume = $item->{'vo'};
			$volume =~ s/-/0/;
			$volume =~ s/m/M/;
			$volume =~ s/k/K/;
			$volume =~ s/,//;
			$volume =~ /.*(M)/;
			if ($1 eq "M") {
				$volume =~ s/M//g;
				$volume = $volume * 1000000;
			} else {
				$volume =~ /.*(K)/;
				if ($1 eq "K") {
					$volume =~ s/K//g;
					$volume = $volume * 1000;
				}	
			}
			my $hi52 = $item->{'hi52'};
			$hi52 =~ s/,//;
			my $lo52 = $item->{'lo52'};
			$lo52 =~ s/,//;

			my $time = $item->{'lt_dts'};
			my $date;
			$time =~ /(.*)T(.*)Z/;
			$time = $2;
			$date = $1;
			my $stmt = qq(INSERT INTO INTRADAY_DATA (ID,NAME,OPEN,HIGH,LOW,LAST,PREVCLOSE,CHANGE,PERCHANGE,VOLUME,HI52,LO52,TIME,DATE)
	VALUES ($id, '$name', $open, $high, $low, $last, $prev_close, $change, $change_per, $volume, $hi52, $lo52, '$time', '$date'));
			my $rv = $dbh->do($stmt) or die $DBI::errstr;
			$id++;
		}

		$dbh->disconnect();
		system("cp tmp_abcdefgh.db realtime_data.db");
	}

	sleep 60;
}

