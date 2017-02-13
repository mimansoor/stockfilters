#/usr/bin/perl

use lib qw( ..);
use LWP::Simple;
use DBI;

use strict;
use warnings;

open (my $fh, '<', 'Company_names.lst') or die "Could not open file 'Company_names.lst' $!";

my $data;
my $filename;
while (my $row = <$fh>) {
	chomp $row;
	my $stock_name = $row;
	my $url = qq(https://www.google.com/finance/getprices?q=$stock_name&x=NSE&i=60&p=15d&f=d,o,h,l,c,v);
	$data = get($url);
	$filename = qq($stock_name.txt);
	open(my $fh1, '>', $filename) or die "Could not open file '$filename' $!";
	print $fh1 $data;
	close $fh1;
}

