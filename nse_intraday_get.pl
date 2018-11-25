#/usr/bin/perl

use lib qw( ..);
use JSON;
use LWP::Simple;
use DBI;
use DateTime qw(:all);
use List::Util qw(shuffle);

my @url;
my @prev_time;
$url[0] = qq(https://nseindia.com/live_market/dynaContent/live_watch/stock_watch/foSecStockWatch.json);
$url[1] = qq(https://nseindia.com/live_market/dynaContent/live_watch/stock_watch/niftyStockWatch.json);

my $URL_SIZE = 2;
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
		#Stop after 15:33PM
		if ($time_now->hour < 9 || $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 17) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 33) {
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

my $same_time_received;

while ($repeat_always) {
	my $start_time = time();
	$same_time_received = 0;
	if (check_for_download_time()) {
		$id = $start_time*1000;
		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;
		my $tmp_url = $url[$toggle_url];

		my $json_str = LWP::Simple::get($tmp_url);
		if (defined $json_str)
		{
			my $data;

			$data = decode_json( $json_str );
			if ($data ne "") {
				my $time = $data->{'time'};
				$time =~ /(.*) (..:..:..)/;
				$time = $2;

				#Dont store if the time is same from previous stored time.
				$same_time_received = 1;
				if ($prev_time[$toggle_url] ne $time) {
					my $date = $1;
					$date =~ s/,//g;
					$date =~ s/ /-/g;
					my @stock_data;

					$prev_time[$toggle_url] = $time;
					
					if ($data->{'latestData'} ne "") {
						#Create item and push for index.
						my @index_data = @{ $data->{'latestData'} };
						my %tmp;

						my $indexName = $index_data[0]->{'indexName'};
						$indexName =~ s/ /_/g;
						$tmp{'symbol'} = $indexName;

						my $indexOpen = $index_data[0]->{'open'};
						$indexOpen =~ s/,//;
						$tmp{'open'} = $indexOpen;

						my $indexHigh = $index_data[0]->{'high'};
						$indexHigh =~ s/,//;
						$tmp{'high'} = $indexHigh;

						my $indexLow = $index_data[0]->{'low'};
						$indexLow =~ s/,//;
						$tmp{'low'} = $indexLow;

						my $indexLtp = $index_data[0]->{'ltp'};
						$indexLtp =~ s/,//;
						$tmp{'ltP'} = $indexLtp;

						my $indexCh = $index_data[0]->{'ch'};
						$indexCh =~ s/,//;
						$tmp{'ptsC'} = $indexCh;

						$tmp{'previousClose'} = $indexLtp - $indexCh;

						$tmp{'per'} = $index_data[0]->{'per'};
						$tmp{'trdVol'} = $data->{'trdVolumesum'};
						$tmp{'wkhi'} = $index_data[0]->{'yHigh'};
						$tmp{'wklo'} = $index_data[0]->{'yLow'};

						push(@stock_data, \%tmp);
					} else {
						@stock_data = @{ $data->{'data'} };
					}

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
						my $change = $item->{'ptsC'};
						$change =~ s/,//;
						my $prev_close = $item->{'previousClose'};
						if ($prev_close ne "") {
							$prev_close =~ s/,//;
						} else {
							$prev_close = $last - $change;
						}
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

						$corporate_action = $item->{'cAct'};

						my $dividend = 0;
						my $ex_date = "";
						if ($corporate_action ne '-') {
							$corporate_action =~ /^(.*) ((.*) PER).*/;
							if ($2 ne "" and $3 ne "") {
								$dividend = sprintf("%.2f",$3);
							}

							$ex_date = $item->{'xDt'};
						}

						my $div_yield = sprintf("%.2f",$dividend/$last * 100.0);


						my $stmt = qq(INSERT INTO INTRADAY_DATA (ID,NAME,OPEN,HIGH,LOW,LAST,PREVCLOSE,CHANGE,PERCHANGE,VOLUME,HI52,LO52,TIME,DATE,Corporate_action,DIV_YIELD,DIVIDEND,EX_DATE)
							VALUES ($id, '$name', $open, $high, $low, $last, $prev_close, $change, $change_per, $volume, $hi52, $lo52, '$time', '$date', '$corporate_action', $div_yield, $dividend,'$ex_date'));
						#my $rv = $dbh->do($stmt) or warn print $stmt."\n",$DBI::errstr,goto to_sleep;
						#print $stmt."\n";
						my $rv = $dbh->do($stmt) or warn print "$stmt\n" and goto to_sleep;
						$id++;
					}

					$toggle_url = ($toggle_url+1) % $URL_SIZE;
					$same_time_received = 0;
				}
			}
		} else {
			warn "Could not open $tmp_url\n";
		}

		$dbh->disconnect();

		#Once we have completed all then only copy
		if ($same_time_received == 0 and $toggle_url == 0) {
			system("cp tmp_abcdefgh.db 01realtime_data.db");
		}
	} else {
		#make sure you sleep for long time if its not yet time.
		$toggle_url = 0;
	}

to_sleep:
	my $end_time = time();
	my $work_time = ($end_time - $start_time);
	my $delay_in_seconds;
	my $sleep_time = 0;

	#if we received same time try again soon
	if ($same_time_received == 1) {
		$delay_in_seconds = 2;
	} else {
		if ($toggle_url == 0) {
			$delay_in_seconds = 90;

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
			$sleep_time = $delay_in_seconds - $work_time + $rounding;
		} else {
			$delay_in_seconds = 5;
		}
	}

	#If for some reason sleep goes negative,
	#Sleep at least delay seconds
	if ($sleep_time <= 0) {
		$sleep_time = $delay_in_seconds;
	}

	#print "Work time: $work_time Sleeping for: $sleep_time\n";

	sleep $sleep_time;
}
