#!/usr/bin/perl
use strict;

#unless(open (FILE1, "< in.csv")) {
unless(open (FILE1, "< symbols.txt")) {
        print "Could not open!\n";
	exit;
}

  
my $high_volume = 450000;
my $liquidity = 1.05;

while(my $line = <FILE1>)
{
	chomp($line);
	#print("drop view \"$line\";\n");

	print("CREATE VIEW '$line' AS SELECT p1.id, p1.name, p1.time, p1.high, printf(\"\%.2f\",(p1.high-p2.high)) as high_change, p1.low, printf(\"\%.2f\",(p1.low-p2.low)) as low_change, p1.last, p1.perchange, printf(\"\%.2f\",(p1.last-p2.last)) as bar_price_change, (p1.volume-p2.volume) as bar_vol_change, printf(\"\%.2f\",((p1.volume-p2.volume)*(p1.last-p2.last))) as bar_turn_over, printf(\"\%.2f\",($high_volume/p1.last)/(p1.volume-p2.volume)) as liquidity_per, (((p1.volume-p2.volume)!=0) AND (abs((p1.volume-p2.volume)*(p1.last-p2.last)) > $high_volume) AND ($high_volume/p1.last/(p1.volume-p2.volume) < $liquidity)) as high_volume FROM INTRADAY_DATA p1, INTRADAY_DATA p2, (SELECT t2.id AS id1, MAX(t1.id) AS id2 FROM INTRADAY_DATA t1, INTRADAY_DATA t2 WHERE t1.name = '$line' AND t2.name = '$line' AND t1.id < t2.id GROUP BY t2.id) AS prev WHERE p1.name = '$line' AND p2.name = '$line' AND p1.id=prev.id1 AND p2.id=prev.id2;\n");
}
close(FILE1);
