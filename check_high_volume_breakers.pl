#! /usr/bin/perl
use lib qw( ..);
use DBI;
use DateTime qw(:all);

use strict;
use warnings;

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
		#Start only after 9:25AM
		#Stop after 15:35PM
		if ($time_now->hour < 9 || $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 25) {
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

#print header
my $html_header = <<EOF;
<!DOCTYPE html>
<html lang="en-US"><head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8">

<meta charset="UTF-8">

<meta name="viewport" content="width=device-width, initial-scale=1">

<title>High Volume Filter | An SQLite database based tool</title>
<br>
EOF

print $html_header."\n";

my $driver   = "SQLite";
my $database = "check_volume.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";

my $repeat_always = 1;
my $delay_in_seconds = 300;

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
while ($repeat_always) {
	if (check_for_download_time()) {
		system("cp 01realtime_data.db check_volume.db");
		#print "Copied<br>\n";

		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;
		#print "Opened database successfully<br>\n";

		my $sth = $dbh->prepare('SELECT * FROM PREV3_MIN_LAST')
				or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		#print "Prev1 Executed<br>\n";
		while (@data = $sth->fetchrow_array()) {
		 $prev3_price{$data[1]} = $data[5];
		 $prev3_vol{$data[1]} = $data[9];
		}

		$sth = $dbh->prepare('SELECT * FROM PREV2_MIN_LAST')
				or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		#print "Prev1 Executed<br>\n";
		while (@data = $sth->fetchrow_array()) {
		 $prev2_price{$data[1]} = $data[5];
		 $prev2_vol{$data[1]} = $data[9];
		}

		$sth = $dbh->prepare('SELECT * FROM PREV1_MIN_LAST')
				or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		#print "Prev1 Executed<br>\n";
		while (@data = $sth->fetchrow_array()) {
		 $prev1_price{$data[1]} = $data[5];
		 $prev1_vol{$data[1]} = $data[9];
		}

		$sth = $dbh->prepare('SELECT * FROM PREV_MIN_LAST')
				or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		#print "Prev Executed<br>\n";
		while (@data = $sth->fetchrow_array()) {
		 $prev_price{$data[1]} = $data[5];
		 $prev_vol{$data[1]} = $data[9];
		}

		$sth = $dbh->prepare('SELECT * FROM LAST_TIME')
			or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		#print "Last Executed<br>\n";
		while (@data = $sth->fetchrow_array()) {
		 $cur_price{$data[1]} = $data[5];
		 $cur_vol{$data[1]} = $data[9];
		 $per_change_in_price{$data[1]} = $data[8];
		}

		$dbh->disconnect();
		#print "Closed database successfully<br>\n";

		my @change_price;
		my @change_volume;
		foreach my $stock_name (sort keys %cur_price)
		{
			#Get prev3 to prev2 change in volume and price
			$change_price[0] =
				$prev2_price{$stock_name} - $prev3_price{$stock_name};
			$change_price[1] =
				$prev1_price{$stock_name} - $prev2_price{$stock_name};
			$change_price[2] =
				$prev_price{$stock_name} - $prev1_price{$stock_name};
			$change_price[3] =
				$cur_price{$stock_name} - $prev_price{$stock_name};

			$change_volume[0] =
				$prev2_vol{$stock_name} - $prev3_vol{$stock_name};
			$change_volume[1] =
				$prev1_vol{$stock_name} - $prev2_vol{$stock_name};
			$change_volume[2] =
				$prev_vol{$stock_name} - $prev1_vol{$stock_name};
			$change_volume[3] =
				$cur_vol{$stock_name} - $prev_vol{$stock_name};

			my $per_change_vol2 = $change_volume[2] == 0? 0 : ($change_volume[3]/$change_volume[2])*100.0;
			my $per_change_vol1 = $change_volume[1] == 0? 0 : ($change_volume[2]/$change_volume[1])*100.0;
			my $per_change_vol0 = $change_volume[0] == 0? 0 : ($change_volume[1]/$change_volume[0])*100.0;

			my $per_change_pr2 = $change_price[2] == 0? 0 : ($change_price[3]/$change_price[2])*100.0;
			my $per_change_pr1 = $change_price[1] == 0? 0 : ($change_price[2]/$change_price[1])*100.0;
			my $per_change_pr0 = $change_price[0] == 0? 0 : ($change_price[1]/$change_price[0])*100.0;

			if ($per_change_vol0 > 3000.0 || $per_change_vol1 > 3000.0 || $per_change_vol2 > 3000.0) {
				printf("<b><a href=\"http://chartink.com/stocks/%s.html\">%s</a></b>(%.02f:%.02f%%): %.02f%% (<b>%.02f%%</b>) %.02f%% (<b>%.02f%%</b>) %.02f%% (<b>%.02f%%</b>)<br>\n",$stock_name,$stock_name,$cur_price{$stock_name},$per_change_in_price{$stock_name},$per_change_pr0,$per_change_vol0,$per_change_pr1,$per_change_vol1,$per_change_pr2,$per_change_vol2);
			}
		}
	}
	sleep $delay_in_seconds;
	my $time_now = DateTime->now( time_zone => 'local' );
	print "<h6>$time_now</h6>\n";
}
