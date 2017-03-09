#/usr/bin/perl

use lib qw( ..);
use JSON;
use LWP::Curl;
use DBI;
use DateTime qw(:all);
use List::Util qw(shuffle);


#Read the Stocks Name and randomize the stock url
open (my $fh, '<', 'mylist.lst') or die "Could not open file 'mylist.lst' $!";

my @mystocks;
my $i = 0;
while (my $row = <$fh>) {
	chomp $row;
	$mystocks[$i] = $row;
	$i++;
}

close $fh;

sub check_for_download_time {
	my $time_now = DateTime->now( time_zone => 'local' );
	my $download = 1;
	my $dow = $time_now->day_of_week;

	#Do not download on Saturday's and Sunday's
	#Saturday == 6, Sunday == 7
	if ($dow == 6 || $dow == 7) {
		$download = 0;
	} else {
		#On Week Day's ie., Monday to Friday
		#Start only after 9:20AM
		#Stop after 15:35PM
		if ($time_now->hour < 9 || $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 17) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 32) {
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
		my $ua = LWP::Curl->new;

		my @shuffled_company = shuffle @mystocks;
		my $url = qq(https://www.google.com/finance/info?infotype=infoquoteall&q=);
		foreach my $row (@shuffled_company) {
			$url .= qq(NSE:$row,);
		}
		$url =~ s/,$//g;

		my $json_str = $ua->get($url);
		if (defined $json_str)
		{
			$json_str =~ s/\/\///;

			my $data;

			$data = decode_json( $json_str );
			if ($data ne "") {
				foreach my $item (@{$data}) {
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
					#my $rv = $dbh->do($stmt) or warn print $stmt."\n",$DBI::errstr,goto to_sleep;
					my $rv = $dbh->do($stmt) or warn print "$stmt\n" and goto to_sleep;
					$id++;
				}
			}
		} else {
			warn "Could not open $url\n";
		}

		$dbh->disconnect();
		system("cp tmp_abcdefgh.db 01realtime_data.db");
	}

to_sleep:
	my $end_time = time();
	my $work_time = ($end_time - $start_time);
	my $delay_in_seconds = 60;

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
