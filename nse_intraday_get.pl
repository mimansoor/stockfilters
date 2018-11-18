#/usr/bin/perl

use lib qw( ..);
use JSON;
use LWP::Simple;
use DBI;
use DateTime qw(:all);
use List::Util qw(shuffle);

my @url;
$url[0] = qq(https://nseindia.com/live_market/dynaContent/live_watch/stock_watch/niftyStockWatch.json);
$url[1] = qq(https://nseindia.com/live_market/dynaContent/live_watch/stock_watch/juniorNiftyStockWatch.json);
$url[2] = qq(https://nseindia.com/live_market/dynaContent/live_watch/stock_watch/niftyMidcap50StockWatch.json);
my $toggle_url = 0;

sub check_for_download_time {
	return 1;
	my $time_now = DateTime->now( time_zone => 'local' );
	my $download = 1;
	my $dow = $time_now->day_of_week;

	#Do not download on Saturday's and Sunday's
	#Saturday == 6, Sunday == 7
	if ($dow == 6 || $dow == 7) {
		$download = 0;
	} else {
		#On Week Day's ie., Monday to Friday
		#Start only after 9:17AM
		#Stop after 15:31PM
		if ($time_now->hour < 9 || $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 17) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 31) {
						$download = 0;
					}
				}
			}
		}
	}

	return $download;
}

system("cp 01realtime_data.db tmp_abcdefgh.db");

my $driver   = "SQLite";
my $database = "tmp_abcdefgh.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";

$repeat_always = 1;

while ($repeat_always) {
	my $start_time = time();
	if (check_for_download_time()) {
		$id = $start_time*1000;
		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;
		my $tmp_url = $url[$toggle_url];
		$toggle_url = ($toggle_url+1) % 3;

		my $json_str = LWP::Simple::get($tmp_url);
		if (defined $json_str)
		{
			my $data;

			$data = decode_json( $json_str );
			if ($data ne "") {
				my @stock_data = @{ $data->{'data'} };
				my $time = $data->{'time'};
				$time =~ /(.*) (..:..:..)/;
				$time = $2;
				my $date = $1;
				$date =~ s/,//g;
				$date =~ s/ /-/g;
				foreach my $item (@stock_data) {
					undef $stmt;
					my $name = $item->{'symbol'};
					$name =~ s/,//;
					my $open = $item->{'open'};
					$open =~ s/,//;
					my $high = $item->{'high'};
					$high =~ s/,//;
					my $low  = $item->{'low'};
					$low =~ s/,//;
					my $last = $item->{'ltP'};
					$last =~ s/,//;
					my $prev_close = $item->{'previousClose'};
					$prev_close =~ s/,//;
					my $change = $item->{'ptsC'};
					$change =~ s/,//;
					my $change_per = $item->{'per'};
					$change_per =~ s/,//;
					my $volume = $item->{'trdVol'};
					$volume =~ s/,//;
					$volume = $volume * 100000; #Convert from Lacs
					my $hi52 = $item->{'wkhi'};
					$hi52 =~ s/,//;
					my $lo52 = $item->{'wklo'};
					$lo52 =~ s/,//;

					$open = $open eq "" ? 0 : $open;
					$high = $high eq "" ? 0 : $high;
					$low = $low eq "" ? 0 : $low;
					$last = $last eq "" ? 0 : $last;
					my $stmt = qq(INSERT INTO INTRADAY_DATA (ID,NAME,OPEN,HIGH,LOW,LAST,PREVCLOSE,CHANGE,PERCHANGE,VOLUME,HI52,LO52,TIME,DATE)
						VALUES ($id, '$name', $open, $high, $low, $last, $prev_close, $change, $change_per, $volume, $hi52, $lo52, '$time', '$date'));
					#my $rv = $dbh->do($stmt) or warn print $stmt."\n",$DBI::errstr,goto to_sleep;
					#print $stmt."\n";
					my $rv = $dbh->do($stmt) or warn print "$stmt\n" and goto to_sleep;
					$id++;
				}
			}
		} else {
			warn "Could not open $tmp_url\n";
		}

		$dbh->disconnect();

		#Once we have completed all 200 then only copy
		if ($toggle_url == 0) {
			system("cp tmp_abcdefgh.db 01realtime_data.db");
		}
	}

to_sleep:
	my $end_time = time();
	my $work_time = ($end_time - $start_time);
	my $delay_in_seconds = 10;

	#To ensure alignment with actual clock if drift
	#in seconds is > delay/2 (seconds)
	my $rounding = $end_time % $delay_in_seconds;
	if ($rounding > $delay_in_seconds/4) {
		$rounding = $delay_in_seconds - $rounding;
	} else {
		$rounding *= -1;
	}

	#Adjust the work time to keep getting every delay(60) seconds
	#And if required move to the next round minute.
	my $sleep_time = $delay_in_seconds - $work_time + $rounding;

	#If for some reason sleep goes negative,
	#Sleep at least delay seconds
	if ($sleep_time <= 0) {
		$sleep_time = $delay_in_seconds;
	}

	print "Sleeping for $sleep_time\n";

	sleep $sleep_time;
}
