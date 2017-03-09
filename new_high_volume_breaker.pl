#! /usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);
use POSIX;

use strict;
use warnings;

my $simulation = 1;
my $send_email = 0;

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
				if ($time_now->minute < 25) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 30) {
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
my $delay_in_seconds = $simulation == 1? 7: 60;

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
my $trade_commission = 200;
my $profit_percentage = 1.5;
my $stop_loss_percentage = 1;

while ($repeat_always) {
	my $start_time = time();

	#Get some unique id value
	my $insert_id = $start_time*1000;

	if (check_for_download_time()) {
		if ($simulation) {
			system("cp realtime_copy.db $database");
		} else {
			system("cp 01realtime_data.db $database");
		}

		#Get the lastest price cached for all stocks.
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

		#Local Cache of Ledger
		#Cache into memory Ledger table
		my %open_trade_entry_price;
		my %open_trade_type;
		my %open_trade_profit_price;
		my %open_trade_stop_loss_price;
		my %open_trade_id;
		my %open_trade_current_price;

		#Just want to create a scope for local variables.
		if (1) {
			my $high_vol_db = "high_volume_calls.db";
			my $dsn = "DBI:$driver:dbname=$high_vol_db";
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;
			my $stmt = qq(SELECT * FROM high_volume_calls_v2 WHERE TRADE_STATUS='OPEN');
			my $sth = $dbh->prepare($stmt) or die $DBI::errstr;
			$sth->execute();
			while (my @data = $sth->fetchrow_array()) {
				$open_trade_id{$data[1]} = $data[0];
				$open_trade_entry_price{$data[1]} = $data[4];
				$open_trade_type{$data[1]} = $data[5];
				$open_trade_profit_price{$data[1]} = $data[6];
				$open_trade_stop_loss_price{$data[1]} = $data[7];

				#Add the last price into ledger
				$open_trade_current_price{$data[1]} = $cur_price{$data[1]};
			}
			$sth->finish();
			$dbh->disconnect();
		}

		#For each stock check if trade triggered
		foreach my $stock_name (sort keys %cur_price)
		{
			#local alias for last price of this stock.
			my $stock_price = $cur_price{$stock_name};

			#Get the last row to see if trade triggered.
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;

			my $stmt = qq(SELECT bar_price_change, high_volume FROM $stock_name WHERE ID=\(SELECT MAX(ID) FROM $stock_name\));

			$sth = $dbh->prepare($stmt)
				or die "Couldn't prepare statement: " . $dbh->errstr;
			$sth->execute();
			my @last_row = $sth->fetchrow_array();
			$sth->finish();
			$dbh->disconnect();
		
			#This is high_volume == 1, if trade triggered.
			if (defined $last_row[1] and $last_row[1] == 1) {
				my $recommendation = "Short_Sell";

				my $stop_loss_price = 0.0;
				my $profit_price = 0.0;

				#this is bar_price_change
				if ($last_row[0] < 0) {
					$recommendation = "Buy";
					$profit_price = sprintf("%.2f",$stock_price*(1+$profit_percentage/100));
					$stop_loss_price = sprintf("%.2f",$stock_price*(1-$stop_loss_percentage/100));
				} else {
					$profit_price = sprintf("%.2f",$stock_price*(1-$profit_percentage/100));
					$stop_loss_price = sprintf("%.2f",$stock_price*(1+$stop_loss_percentage/100));
				}

				my $report_simulation = "";
				if ($simulation) {
					$report_simulation = "[Simulation Ignore]";
				}

				#Check with current open trades if its same direction then ignore.
				if (!defined $open_trade_type{$stock_name} || $open_trade_type{$stock_name} ne $recommendation) {
					#Check if its new trade recommendation.
					my $new_trade = 0;
					if (!defined $open_trade_type{$stock_name}) {
						$new_trade = 1;
					}

					#If its not a new trade then its Cover and Reverse.
					my $email_recommendation = $new_trade == 1 ? $recommendation : "Cover Old And $recommendation";

					my $email_cmd = qq(-s "Fourways Profit: $report_simulation $time_of_last_price{$stock_name}: $email_recommendation $stock_name \($stock_price\) Target: $profit_price StopLoss: $stop_loss_price" mimansoor\@gmail.com,lksingh74\@gmail.com < /dev/null);
					my $email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;

					#Now store it in a DB with Buy, take profit and stop loss values
					my $database = "high_volume_calls.db";
					my $dsn = "DBI:$driver:dbname=$database";
					my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, AutoCommit => 1 })
							      or die $DBI::errstr;

					#If New Trade just Insert Row
					if ($new_trade) {
						my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME)
									VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, 0.0, 'OPEN', $stock_price, '$time_of_last_price{$stock_name}'));
						my $rv = $dbh->do($insert_stmt) or warn print "$insert_stmt\n";
					} else {
						#Close the old trade, and and new trade.
						my $profit_loss = 0;
						my $quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});
						if ($open_trade_type{$stock_name} eq 'Buy') {
							$profit_loss = sprintf("%.2f",(($stock_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
						} else {
							$profit_loss = sprintf("%.2f",(($open_trade_entry_price{$stock_name} - $stock_price)*$quantity-$trade_commission));
						}
						my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = 'CLOSED',
								 CURRENT_PRICE = $stock_price, PROFIT_LOSS = $profit_loss,
								 EXIT_TIME = '$time_of_last_price{$stock_name}' WHERE ID == $open_trade_id{$stock_name};);
						my $my_ledger_rv = $dbh->do($update_stmt) or die $DBI::errstr;

						#Now add the new record
						my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME)
									VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, -200.0, 'OPEN', $stock_price, '$time_of_last_price{$stock_name}'));

						#make sure update last_price code doesn't see this
						undef $open_trade_type{$stock_name};
					}

					$dbh->disconnect();
					$insert_id++;
				}
			}

			#Update last_price and check if we need to close the trade due to profit or stop loss trigger.
			if (defined $open_trade_type{$stock_name}) {
				my $database = "high_volume_calls.db";
				my $dsn = "DBI:$driver:dbname=$database";
				my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, AutoCommit => 1 })
						      or die $DBI::errstr;
				my $trade_status = "OPEN";

				my $profit_loss = 0;
				my $quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});

				if ($open_trade_type{$stock_name} eq "Buy") {
					if (($open_trade_profit_price{$stock_name} <= $stock_price) ||
					    ($open_trade_stop_loss_price{$stock_name} >= $stock_price)) {
						$trade_status = "CLOSED";
					}
					my $closing_price = ($open_trade_profit_price{$stock_name} <= $stock_price) ? $open_trade_profit_price{$stock_name} :
							    ($open_trade_stop_loss_price{$stock_name} >= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;

					$profit_loss = sprintf("%.2f",(($closing_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
				} else {
					if (($open_trade_profit_price{$stock_name} >= $stock_price) ||
					    ($open_trade_stop_loss_price{$stock_name} <= $stock_price)) {
						$trade_status = "CLOSED";
					}

					my $closing_price = ($open_trade_profit_price{$stock_name} >= $stock_price) ? $open_trade_profit_price{$stock_name} :
							    ($open_trade_stop_loss_price{$stock_name} <= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;

					$profit_loss = sprintf("%.2f",(($open_trade_entry_price{$stock_name} - $closing_price)*$quantity-$trade_commission));
				}

				my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = '$trade_status',
							CURRENT_PRICE = $stock_price, PROFIT_LOSS = $profit_loss,
							EXIT_TIME = '$time_of_last_price{$stock_name}' WHERE ID == $open_trade_id{$stock_name};);
				my $my_ledger_rv = $dbh->do($update_stmt) or die $DBI::errstr;

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

	#just offset by some sec's with realtime_1_min_intraday_data
	$sleep_time = $simulation == 1 ? $sleep_time : $sleep_time + 10;
	print "Sleeping for $sleep_time\n";
	sleep $sleep_time;
}
