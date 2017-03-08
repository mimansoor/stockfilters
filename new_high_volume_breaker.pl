#! /usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);

use strict;
use warnings;

my $simulation = 1;
my $send_email = 1;

sub check_for_download_time {
	if ($simulation) {
		return 1;
	}
	my $time_now = DateTime->now( time_zone => 'local' );
	my $download = 1;
	my $dow = $time_now->day_of_week;

	#Do not download on Saturday's and Sunday's
	#Saturday == 6, Sunday == 7
	if ($dow == 6 || $dow == 7) {
		$download = 0;
	} else {
		#On Week Day's ie., Monday to Friday
		#Start only after 9:25AM
		#Stop after 15:35PM
		if ($time_now->hour < 9 || $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 30) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 35) {
						$download = 0;
					}
				}
			}
		}
	}

	return $download;
}


#Main Starts Here
STDOUT->autoflush(1);

my $driver   = "SQLite";
my $database = "check_volume1.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";

my $repeat_always = 1;
my $delay_in_seconds = $simulation == 1? 20: 60;

my $email_program = "mutt";

my @data;
my %prev3_price;
my %prev3_vol;
my %prev2_price;
my %prev2_vol;
my %prev1_price;
my %prev1_vol;
my %prev_price;
my %prev_vol;
my %cur_price;
my %cur_vol;
my %per_change_in_price;
my %time_of_last_price;
my %date_of_last_price;
my $lot_size_in_cash = 200000;
my $max_per_lot_size_to_vol = 5;


while ($repeat_always) {
	my $start_time = time();

	if (check_for_download_time()) {
		if ($simulation) {
			system("cp realtime_copy.db $database");
		} else {
			system("cp 01realtime_data.db $database");
		}

		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;

		my $sth = $dbh->prepare('SELECT * FROM LAST_TIME')
			or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		while (@data = $sth->fetchrow_array()) {
		 $cur_price{$data[1]} = $data[5];
		 $per_change_in_price{$data[1]} = $data[8];
		 $time_of_last_price{$data[1]} = $data[12];
		 $date_of_last_price{$data[1]} = $data[13];
		}

		$dbh->disconnect();

		foreach my $stock_name (sort keys %cur_price)
		{

			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;

			my $stmt = qq(SELECT bar_price_change, high_volume FROM $stock_name WHERE ID=\(SELECT MAX(ID) FROM $stock_name\));

			$sth = $dbh->prepare($stmt)
				or die "Couldn't prepare statement: " . $dbh->errstr;
			$sth->execute();
			my @last_row = $sth->fetchrow_array();
			$sth->finish();
			$dbh->disconnect();
		
			if ($last_row[1] == 1) {
				my $recommendation = "Sell";
				if ($last_row[0] < 0) {
					$recommendation = "Buy";
				}
				my $email_cmd = qq(-s "Fourways Profit: $time_of_last_price{$stock_name}: $recommendation $stock_name \($cur_price{$stock_name}\)" mimansoor\@gmail.com,lksingh74\@gmail.com < /dev/null);
				my $email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;

				#Now store it in a DB with Buy, take profit and stop loss values
				my $driver   = "SQLite";
				my $database = "high_volume_calls.db";
				my $dsn = "DBI:$driver:dbname=$database";
				my $userid = "";
				my $password = "";
				my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
						      or die $DBI::errstr;
				#Get some unique id value
				my $insert_id = time();
				my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, TIME, PRICE, TRADE_TYPE)
							VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $cur_price{$stock_name}, '$recommendation'));
				my $rv = $dbh->do($insert_stmt) or warn print "$insert_stmt\n";
				$dbh->disconnect();
			}
		}
	}

to_sleep:
	my $end_time = time();
	my $work_time = ($end_time - $start_time);

	#To ensure alignment with actual clock if drift
	#in seconds is > delay/2 (seconds)
	my $rounding = $end_time % $delay_in_seconds;
	if ($rounding > $delay_in_seconds/2) {
		$rounding = $delay_in_seconds - $rounding;
	} else {
		#Decrease time from sleep
		$rounding *= -1.0;
	}

	#Adjust the work time to keep getting every delay(300) seconds
	#And if required move to the next round minute.
	my $sleep_time = $delay_in_seconds - $work_time + $rounding;

	#If for some reason sleep goes negative,
	#Sleep at least delay seconds
	if ($sleep_time <= 0) {
		$sleep_time = $delay_in_seconds;
	}

	#just offset by 5sec with realtime_1_min_intraday_data
	$sleep_time += 5;
	print "Sleeping for $sleep_time\n";
	sleep $sleep_time;
}
