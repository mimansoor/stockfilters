#/usr/bin/perl

use lib qw( ..);
use LWP::Simple;
use JSON;
use LWP::Simple;
use DBI;

my $url = "http://www.google.com/finance/info?infotype=infoquoteall&q=NASDAQ:AAPL,NASDAQ:INTC";

#my $header = "time,open,high,low,close,prevclose,change,perchange,volume,hi52,lo52,\n";
#print $header;

my $driver   = "SQLite";
my $database = "index_data.db";
my $dsn = "DBI:$driver:dbname=$database";
my $userid = "";
my $password = "";
my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
                      or die $DBI::errstr;
print "Opened database successfully\n";

$id = 2;

while (1) {
	my $json_str = get($url);
	die "Couldn't get $url" unless defined $json_str;

	$json_str =~ s/\/\///;

	my $data;

	$data = decode_json( $json_str );
	foreach my $item (@{$data}) {
	   	#print "$item->{'ltt'},$item->{'op'},$item->{'hi'},$item->{'lo'},$item->{'l'},$item->{'vo'}\n";
		undef $stmt;
		my $name = $item->{'t'};
		my $open = $item->{'op'};
		my $high = $item->{'hi'};
		my $low  = $item->{'lo'};
		my $last = $item->{'l'};
		my $prev_close = $item->{'pcls_fix'};
		my $change = $item->{'c_fix'};
		my $change_per = $item->{'cp_fix'};
		my $volume = $item->{'vo'};
		$volume =~ s/-/0/;
		my $hi52 = $item->{'hi52'};
		my $lo52 = $item->{'lo52'};
		my $stmt = qq(INSERT INTO INTRADAY_DATA (ID,NAME,OPEN,HIGH,LOW,LAST,PREVCLOSE,CHANGE,PERCHANGE,VOLUME,HI52,LO52)
VALUES ($id, '$name', $open, $high, $low, $last, $prev_close, $change, $change_per, $volume, $hi52, $lo52));
		#print $stmt;
		my $rv = $dbh->do($stmt) or die $DBI::errstr;
		$id++;
	}

	sleep 60;
}
