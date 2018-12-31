#! /usr/bin/perl

use strict;
use warnings;

#Nifty weight token fields
my $NIFTY_W_SYMB = 0;
my $NIFTY_W_WEIGHT = 1;

open (my $fh, '<', 'nifty_weight.csv') or die "Could not open file 'nifty_weight.csv' $!";
while (my $line = <$fh>) {
	chomp $line;
	my @tokens = split/,/, $line;

	printf("SYMBL = $tokens[$NIFTY_W_SYMB] weight = $tokens[$NIFTY_W_WEIGHT]\n");
}


