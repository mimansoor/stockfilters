#/usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);
use POSIX;

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

#Open My_Simulation_Portfolio database
my $simulation_portfolio = "my_simulation_portfolio.db";
my $dsn2 = "DBI:$driver:dbname=$simulation_portfolio";
my $dbh2 = DBI->connect($dsn2, $userid, $password, { RaiseError => 1, AutoCommit => 1})
                      or die $DBI::errstr;
my $stock_insert_id = 1;
my $per_lot_cash_to_use = 100000;
my $per_lot_commission = 200;
#print "Opened $simulation_portfolio successfully\n";


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
print "Done: Collecting number of rows for each stock\n";

my $cur_row = 0;
my $wait_rows = 0;
my $delay_in_seconds = 0;
my %stocks_bought;
my %stocks_buyprice;
my %stocks_buyid;
my %stocks_buyquantity;
my $total_cost = 0.0;
my $current_total;
my $row_time;

while ($cur_row < $max_rows) {
	my $start_time = time();

	#Insert into test_realtime.db one by one for each stock
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

	$cur_row++;

	system("cp test_realtime.db realtime_copy.db");

	#Momentum strategy: Buy the top stock, keep it till it falls below Nth rank
	my $max_n_stocks = 10; #Note: make sure this is not more than 10-15
	if ($cur_row > $wait_rows) {
		my $stmt = qq(SELECT * FROM "latest_high_low_per" LIMIT 0, $max_n_stocks);
		my $sth = $dbh1->prepare($stmt) or warn print "$stmt\n" and continue;
		$sth->execute();
		
		#Buy the top stock N stocks
		my $top_n_to_buy = 1;
		my $buy_count = 0;
		while (my @rows = $sth->fetchrow_array()) {
			my $found = 0;
			$row_time = $rows[12];
			foreach my $stock_name (keys %stocks_bought) {
				if ($rows[1] eq $stock_name) {
					$found = 1;
					last;
				}
			}
			if ($found == 0) {
				$stocks_bought{$rows[1]} = $per_lot_cash_to_use;	
				$stocks_buyid{$rows[1]} = $stock_insert_id;
				$stocks_buyprice{$rows[1]} = $rows[5];	
				$total_cost += ($per_lot_cash_to_use + $per_lot_commission);
				print "****------Buy $rows[1]@ $rows[5] --------****\n";

				#Now insert this into my_ledger table.
				my $stock_quantity = floor($per_lot_cash_to_use/$rows[5]);
				my $stock_buy_price = sprintf("%.2f",($stock_quantity*$rows[5] + $per_lot_commission)/$stock_quantity);
				my $stmt = qq(INSERT INTO my_ledger (ID, NAME, TRADE_STATUS, BUY_PRICE, SELL_PRICE, QUANTITY, BUY_TIME, SELL_TIME)
					VALUES ($stock_insert_id, '$rows[1]', 'OPEN', $stock_buy_price, $rows[5], $stock_quantity, '$row_time', '$row_time'));
				my $my_ledger_rv = $dbh2->do($stmt) or die $DBI::errstr;

				$stocks_buyquantity{$rows[1]} = $stock_quantity;
				$stock_insert_id++;
			}

			$buy_count++;
			if ($buy_count >= $top_n_to_buy) {
				last;
			}
		}

		undef $buy_count;
		undef $top_n_to_buy;

		#Sell the stock that has fallen out of top N

		#Catch the 100 rows first
		$current_total = 0.0;
		$stmt = qq(SELECT * FROM "latest_high_low_per" LIMIT 0, 100);
		foreach my $stock_name (keys %stocks_bought) {
			my $found = 0;
			my $sell_price = 0;
			my $sell_time;

			$sth = $dbh1->prepare($stmt) or warn print "$stmt\n" and continue;
			$sth->execute();
			my $i = 0;
			while (my @rows = $sth->fetchrow_array()) {
				if (($i < $max_n_stocks) && ($stock_name eq $rows[1])) {
					$found = 1;	

					$current_total += $stocks_bought{$stock_name}*($rows[5]/$stocks_buyprice{$stock_name});

					#Now Update current price into my_ledger table.
					my $stmt = qq(UPDATE my_ledger set SELL_PRICE = $rows[5], SELL_TIME = '$rows[12]' WHERE ID == $stocks_buyid{$stock_name};);
					my $my_ledger_rv = $dbh2->do($stmt) or die $DBI::errstr;

					last;
				} else {
					if ($stock_name eq $rows[1]) {
						$sell_price = $rows[5];
						$sell_time = $rows[12];

						last;
					}
				}
				$i++;
			}

			if ($found == 0) {
				print "****------Sell $stock_name@ $sell_price-----****\n";
				$current_total += $stocks_bought{$stock_name}*($sell_price/$stocks_buyprice{$stock_name});
				$current_total -= $stocks_bought{$stock_name};
				$total_cost += ($stocks_bought{$stock_name}*($sell_price/$stocks_buyprice{$stock_name} - 1))*-1;
				$total_cost -= $stocks_bought{$stock_name};
				delete($stocks_bought{$stock_name});
				delete($stocks_buyprice{$stock_name});

				#Now Update sell into my_ledger table.
				my $stock_quantity = $stocks_buyquantity{$stock_name};
				my $stock_buy_price = $stocks_buyprice{$stock_name};
				my $stmt = qq(UPDATE my_ledger set TRADE_STATUS = 'CLOSED', SELL_PRICE = $sell_price, SELL_TIME = '$sell_time' WHERE ID == $stocks_buyid{$stock_name};);
				my $my_ledger_rv = $dbh2->do($stmt) or die $DBI::errstr;
			}
		}
	
	}


to_sleep:
	print "Completed $cur_row pass at Time: $row_time\n";
	print "Current Value: $current_total\n";
	print "Cost Price: $total_cost\n";
	print "Total Profit: ".((($current_total/$total_cost))*100.0)."%\n";

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
	if ($sleep_time > 0) {
		sleep $sleep_time;
	}
}

#close database section
jmp_to_close:
$dbh->disconnect();
#print "Closed $database successfully\n";
$dbh1->disconnect();
#print "Closed $realtime_db successfully\n";
$dbh2->disconnect();
#print "Closed $simulation_portfolio successfully\n";
