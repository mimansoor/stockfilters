#/usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);

use strict;
use warnings;

my $driver   = "SQLite";
my $userid = "";
my $password = "";



#Open source database
#my $database = "intraday_data.db";
my $database = "01realtime_data.db";
my $dsn = "DBI:$driver:dbname=$database";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
                      or die $DBI::errstr;
#print "Opened $database successfully\n";

#Open simulation database
my $realtime_db = "test_realtime.db";
my $dsn1 = "DBI:$driver:dbname=$realtime_db";
my $dbh1 = DBI->connect($dsn1, $userid, $password, { RaiseError => 1, AutoCommit => 1})
                      or die $DBI::errstr;
#print "Opened $realtime_db successfully\n";


#Main begins here
#Algo: 
#	Open source database
#	Open simulation database
#	Count number of records in each view and
#	store it in associative array by name.

#print "Collecting number of rows for each stock\n";
#open (my $fh, '<', 'C.csv') or die "Could not open file 'C.csv' $!";
open (my $fh, '<', 'D.csv') or die "Could not open file 'D.csv' $!";
my %count_rows;
my %completed_rows;
my $max_rows = -1;
while (my $name = <$fh>) {
	chomp $name;

	my $stmt = qq(SELECT count(*) from '$name');
	my $sth = $dbh->prepare($stmt) or warn print "$stmt\n" and continue;
	$sth->execute();
	my $data = $sth->fetchrow_array();
	$count_rows{$name} = $data;
	$completed_rows{$name} = 0;
	if ($max_rows < $data) {
		$max_rows = $data;
	}
#	print $name."\n";
}
out_label:
close $fh;
#print "Done: Collecting number of rows for each stock\n";

my $cur_row = 0;
my $delay_in_seconds = 20;
while ($cur_row < $max_rows) {
	my $start_time = time();
	foreach my $stock_name (sort keys %count_rows)
	{
		my $num_rows = $count_rows{$stock_name};
		if ($completed_rows{$stock_name} < $num_rows) {
			my $stmt = qq(SELECT * from INTRADAY_DATA where name='$stock_name' order by time asc limit $completed_rows{$stock_name}, 1);
			my $sth = $dbh->prepare($stmt) or warn print "$stmt\n" and continue;
			$sth->execute();
			my @data = $sth->fetchrow() and $sth->finish();
			
			#my $insert_stmt = qq(INSERT INTO INTRADAY_GETPRICES_BACKFILL (ID, DATE, TIME, NAME, CLOSE, HIGH, LOW, OPEN, VOLUME)
					#VALUES ($data[0], '$data[1]', '$data[2]', '$data[3]', $data[4], $data[5], $data[6], $data[7], $data[8]));
			my $insert_stmt = qq(INSERT INTO INTRADAY_DATA (ID, NAME, OPEN, HIGH, LOW, LAST, PREVCLOSE, CHANGE, PERCHANGE, VOLUME, HI52, LO52, TIME, DATE)
					VALUES ($data[0], '$data[1]', $data[2], $data[3], $data[4], $data[5], $data[6], $data[7], $data[8], $data[9], $data[10], $data[11], '$data[12]', '$data[13]'));
			#print "$insert_stmt\n";
			my $rv = $dbh1->do($insert_stmt) or warn print "$insert_stmt\n" and continue;
			$completed_rows{$stock_name}++;
		}
	}

	system("cp test_realtime.db realtime_copy.db");
	my $time_now = DateTime->now( time_zone => 'local' );
	#print "$time_now: Completed $cur_row pass\n";
	$cur_row++;

to_sleep:
	my $end_time = time();
	my $work_time = ($end_time - $start_time);

	#To ensure alignment with actual clock if drift
	#in seconds is > delay/2 (seconds)
	my $rounding = 0;
	if ($delay_in_seconds != 0) {
		$rounding = $end_time % $delay_in_seconds;
	}
	#print "Rounding: $rounding\n";
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
	if ($sleep_time < 0) {
		$sleep_time = $delay_in_seconds;
	}

	#print "Sleeping for $sleep_time\n";
	if ($sleep_time != 0) {
		#print "Enter to continue: ";
		#my $prompt = <STDIN>;
		sleep $sleep_time;
	}
}

#close database section
jmp_to_close:
$dbh->disconnect();
print "Closed $database successfully\n";
$dbh1->disconnect();
print "Closed $realtime_db successfully\n";
