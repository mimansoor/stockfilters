#/usr/bin/perl

use lib qw( ..);
use strict;
use warnings;
use JSON;
use LWP::Simple;
use DBI;
use DateTime qw(:all);
use List::Util qw(shuffle);
use Text::CSV;

my $csv = Text::CSV->new ({
  binary    => 1,
  auto_diag => 1,
  sep_char  => ','    # not really needed as this is the default
});

use Text::ParseWords;
sub parse_csv1 {
	my $line = shift(@_);
	$csv->parse($line);
	return $csv->fields();
}

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

#First read into memory NSE symbol to IN name
my $NIFTY_SYMB = 0;
my $NIFTY_IN_SYMB = 1;
my %insymbols_map;

open (my $fh, '<', 'investing.csv') or die "Could not open file 'investing.csv' $!";
while (my $line = <$fh>) {
	chomp $line;
	my @tokens = parse_csv1($line);
	$insymbols_map{$tokens[$NIFTY_IN_SYMB]} = $tokens[$NIFTY_SYMB];
}
close($fh);

system("cp inrealtime_data.db tmp_inrealtime.db");

my $driver   = "SQLite";
my $database = "tmp_inrealtime.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";

my $repeat_always = 1;

my $start_time;

#In table columns
my $IN_SYMBOL=0;
my $IN_LAST=1;
my $IN_HIGH=2;
my $IN_LOW=3;
my $IN_CHNG=4;
my $IN_CHNG_PER=5;
my $IN_VOLUME=6;
my $IN_TIME=7;

while ($repeat_always) {
	$start_time = time();

	if (check_for_download_time()) {
		my $id = time()*1000;
		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;

		#Download the file from in.investing and convert to csv file
		system(". ./get_nifty_200.sh");

		#Now read the csv file into memory
		open (my $fh, '<', 'nifty_200.csv') or die "Could not open file 'nifty_200.csv' $!";
		my @stock_data;

		#Get todays date.
		my ($tsec, $tmin, $thour, $tmday, $tmon, $tyear, $twday, $tyday, $tisdst) = localtime(time);
		my $today_mon = $tmon+1;
		my $today_year = $tyear+1900;
		my $todays_date = "$tmday-$today_mon-$today_year";
		while (my $line = <$fh>) {
			my %in_data;
			my @tokens = parse_csv1($line);
			my $nse_symbol;
		       	!defined $tokens[$IN_SYMBOL] and next;
		       	$nse_symbol = $insymbols_map{$tokens[$IN_SYMBOL]};
			(!defined $nse_symbol or $nse_symbol eq '') and next;
			$in_data{'time'} = $tokens[$IN_TIME];
			$in_data{'symbol'} = $nse_symbol;
			$tokens[$IN_LAST] =~ s/,//;
			$in_data{'open'} = $tokens[$IN_LAST];
			$tokens[$IN_HIGH] =~ s/,//;
			$in_data{'high'} = $tokens[$IN_HIGH];
			$tokens[$IN_LOW] =~ s/,//;
			$in_data{'low'} = $tokens[$IN_LOW];
			$in_data{'ltP'} = $tokens[$IN_LAST];
			$tokens[$IN_CHNG] =~ s/,//;
			my $val = ($tokens[$IN_CHNG] =~ /(\+-(\d*\.?\d*))/);
			if ($val) {
				$tokens[$IN_CHNG] = $2;
			} else {
				$val = ($tokens[$IN_CHNG] =~ /(\+(\d*\.?\d*))/);
				if ($val) {
					$tokens[$IN_CHNG] = $2;
				}
			}
			$in_data{'ptsC'} = int($tokens[$IN_CHNG]);
			$tokens[$IN_CHNG_PER] =~ s/,//;
			$tokens[$IN_CHNG_PER] =~ s/%//;
			$val = ($tokens[$IN_CHNG_PER] =~ /(\+-(\d*\.?\d*))/);
			if ($val) {
				$tokens[$IN_CHNG_PER] = $2;
			} else {
				$val = ($tokens[$IN_CHNG_PER] =~ /(\+(\d*\.?\d*))/);
				if ($val) {
					$tokens[$IN_CHNG_PER] = $2;
				}
			}
			$in_data{'per'} = $tokens[$IN_CHNG_PER];
			$tokens[$IN_VOLUME] =~ s/\"//;
			$tokens[$IN_VOLUME] =~ s/,//;
			$val = ($tokens[$IN_VOLUME] =~ /((\d*\.?\d*)K)/);
			if ($val) {
				$tokens[$IN_VOLUME] = $2*1000;
			} else {
				my $val = ($tokens[$IN_VOLUME] =~ /((\d*\.?\d*)M)/);
				if ($val) {
					$tokens[$IN_VOLUME] = $2*1000000;
				}
			}
			$in_data{'trdVol'} = $tokens[$IN_VOLUME]*1.0;

			#Now push this in array of stock_data
			push @stock_data, \%in_data;
		}
		close($fh);

		#Dont store if the time is same from previous stored time.
		foreach my $item (@stock_data) {
			my $time = $item->{'time'};
			my $name = $item->{'symbol'};
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
			my $change_per = $item->{'per'};
			$change_per =~ s/,//;
			my $volume = $item->{'trdVol'};

			$open = $open eq "" ? 0 : $open;
			$high = $high eq "" ? 0 : $high;
			$low = $low eq "" ? 0 : $low;
			$last = $last eq "" ? 0 : $last;

			my $stmt = qq(INSERT INTO INTRADAY_DATA (ID,NAME,HIGH,LOW,LAST,CHANGE,PERCHANGE,VOLUME,TIME,DATE)
				VALUES ($id, '$name', $high, $low, $last, $change, $change_per, $volume, '$time', '$todays_date'));
			#my $rv = $dbh->do($stmt) or warn print $stmt."\n",$DBI::errstr,goto to_sleep;
			#print $stmt."\n";
			my $rv = $dbh->do($stmt) or warn print "$stmt\n" and goto to_sleep;
			$id++;
		}

		$dbh->disconnect();

		#Once we have completed all then only copy
		system("cp tmp_inrealtime.db inrealtime_data.db");
	}

to_sleep:
	my $delay_in_seconds = 0;
	my $sleep_time = 0;
	my $work_time = 0;

	my $end_time = time();
	$work_time = ($end_time - $start_time);
	$delay_in_seconds = 180;

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

	#If for some reason sleep goes negative,
	#Sleep at least delay seconds
	if ($sleep_time <= 0) {
		$sleep_time = $delay_in_seconds;
	}

	#print "Work time: $work_time Sleeping for: $sleep_time\n";
	sleep $sleep_time;
}
