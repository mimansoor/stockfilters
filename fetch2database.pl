#/usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);

use strict;
use warnings;

my $driver   = "SQLite";
my $database = "intraday_data.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
                      or die $DBI::errstr;
print "Opened database successfully\n";



open (my $fh, '<', 'Company_names.lst') or die "Could not open file 'Company_names.lst' $!";

my $data;
my $filename;
my $full_time=0;
my $id=887357+1;
while (my $row = <$fh>) {
	chomp $row;
	my $stock_name = $row;
	$filename = qq($stock_name.txt);
	open(my $fh1, '<', $filename) or die "Could not open file '$filename' $!";
	my $line;
	$line = <$fh1>; #Remove EXCHANGE line
	$line = <$fh1>; #Remove MARKET_OPEN_MINUTE
	$line = <$fh1>; #Remove MARKET_CLOSE_MINUTE
	$line = <$fh1>; #Remove INTERVAL=60
	$line =~ /INTERVAL=(.*)/;
	my $interval = $1;
	$line = <$fh1>; #Remove COLUMNS
	$line = <$fh1>; #Remove DATA=
	$line = <$fh1>; #Remove TIMEZONE_OFFSET
	$line =~ /TIMEZONE_OFFSET=(.*)/;
	my $timezone_offset = $1;
	while ($line = <$fh1>) {
		# Get the tokens first
		my @tokens = split/,/, $line;
		my $time = $tokens[0];
		if ($time =~ /a.*/) {
			$time =~ /a(.*)/;
			$full_time = $1;
			$time =~ s/a//;
		} else {
			$time = $full_time + $interval*$time;
		}

		$time += $timezone_offset*60;

		my $dt = DateTime->from_epoch( epoch => $time);
		$dt =~ /(.*)T(.*)/;
		$time = $2;
		my $date = $1;

		my $stmt = qq(INSERT INTO INTRADAY_GETPRICES_BACKFILL (ID,DATE,TIME,NAME,CLOSE,HIGH,LOW,OPEN,VOLUME)
VALUES ($id, '$date', '$time', '$stock_name', $tokens[1], $tokens[2], $tokens[3], $tokens[4], $tokens[5]));
		my $rv = $dbh->do($stmt) or die $DBI::errstr;

		$id++;
	}
	print "Completed importing $stock_name\n";
	
	close $fh1;
}

$dbh->disconnect();
print "Closed database successfully\n";
