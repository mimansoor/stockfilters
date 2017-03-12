#! /usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);
use POSIX;

use strict;
use warnings;

my $simulation = 1;
my $send_email = 0;

#View filed indexes
my $STOCK_ID 		= 0;
my $STOCK_NAME 		= 1;
my $STOCK_TIME 		= 2;
my $STOCK_HIGH 		= 3;
my $STOCK_HI_CNG 	= 4;
my $STOCK_LOW 		= 5;
my $STOCK_LO_CNG 	= 6;
my $STOCK_LAST 		= 7;
my $STOCK_PERCNG 	= 8;
my $STOCK_BAR_PR_CNG 	= 9;
my $STOCK_BAR_VOL_CNG 	= 10;
my $STOCK_BAR_TURNOVER 	= 11;
my $STOCK_BAR_LIQDITY 	= 12;
my $STOCK_HIGH_VOL 	= 13;

sub check_for_download_time {
	if ($simulation) {
		return 1;
	}
	my $time_now = DateTime->now( time_zone => 'local' );
	my $download = 1;
	my $dow = $time_now->day_of_week;

	#Do not download on Saturday's and Sunday's
	#Saturday == 6, Sunday == 7
	if ($dow == 6 or $dow == 7) {
		$download = 0;
	} else {
		#On Week Day's ie., Monday to Friday
		#Start only after 9:18AM
		#Stop after 15:30PM
		if ($time_now->hour < 9 or $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 18) {
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
my $delay_in_seconds = $simulation == 1? 7: 30;

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
my %low_price;
my %high_price;
my %per_change_in_price;
my %time_of_last_price;
my %date_of_last_price;
my $lot_size_in_cash = 300000;
my $trade_commission = 200;
my $stop_loss_percentage = 0.25;
my $profit_percentage = $stop_loss_percentage*2;
my $low_threshold = 0.25;
my $high_threshold = 0.25;

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
		 $high_price{$data[1]} = $data[3];
		 $low_price{$data[1]} = $data[4];
		 $cur_price{$data[1]} = $data[5];
		 $per_change_in_price{$data[1]} = $data[8];
		 $time_of_last_price{$data[1]} = $data[12];
		 $date_of_last_price{$data[1]} = $data[13];
		}

		$dbh->disconnect();

		#Local Cache of Ledger
		#Cache into memory Ledger table
		my %open_trade_entry_price;
		my %open_trade_entry_time;
		my %open_trade_type;
		my %open_trade_profit_price;
		my %open_trade_stop_loss_price;
		my %open_trade_id;
		my %open_trade_current_price;
		my %open_trade_low;
		my %open_trade_high;

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
				$open_trade_entry_time{$data[1]} = $data[3];
				$open_trade_entry_price{$data[1]} = $data[4];
				$open_trade_type{$data[1]} = $data[5];
				$open_trade_profit_price{$data[1]} = $data[6];
				$open_trade_stop_loss_price{$data[1]} = $data[7];

				#Add the high,low and last, price into ledger
				$open_trade_current_price{$data[1]} = $cur_price{$data[1]};
				$open_trade_low{$data[1]} = $low_price{$data[1]};
				$open_trade_high{$data[1]} = $high_price{$data[1]};
			}
			$sth->finish();
			$dbh->disconnect();
		}

		#Cache into memory Close Trade Ledger for this stock.
		my %close_trade_exit_time;

		#Just want to create a scope for local variables.
		if (1) {
			my $high_vol_db = "high_volume_calls.db";
			my $dsn = "DBI:$driver:dbname=$high_vol_db";
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;
			my $stmt = qq(SELECT * FROM high_volume_calls_v2 WHERE TRADE_STATUS='CLOSED' order by exit_time asc);
			my $sth = $dbh->prepare($stmt) or die $DBI::errstr;
			$sth->execute();

			#Let it overwrite the exit_time so we get the last trade exit time only.
			while (my @data = $sth->fetchrow_array()) {
				$close_trade_exit_time{$data[1]} = $data[11];
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

			my $stmt = qq(SELECT * FROM $stock_name WHERE ID=\(SELECT MAX(ID) FROM $stock_name\));

			$sth = $dbh->prepare($stmt)
				or die "Couldn't prepare statement: " . $dbh->errstr;
			$sth->execute();
			my @last_row = $sth->fetchrow_array();
			$sth->finish();
			$dbh->disconnect();
		
			#This is high_volume == 1, if trade triggered.
			#Buy/Sell Algo
			#	Buy: When you find a Non-zero high_change with high_volumes
			#	Sell: When you find a Non-zero low_change with high_volumes
			if ((defined $last_row[$STOCK_HIGH_VOL]) and ($last_row[$STOCK_HIGH_VOL] == 1)
				and ($last_row[$STOCK_LO_CNG] != 0 or $last_row[$STOCK_HI_CNG] != 0)) {
				my $recommendation = "Short_Sell";

				my $stop_loss_price = 0.0;
				my $profit_price = 0.0;

				my $trade_trigerred = 0;
				#Dont enter if we closed the position just in the last bar,
				#It is possible to see this since we sample twice.
				if ((!defined $close_trade_exit_time{$stock_name} or ($close_trade_exit_time{$stock_name} ne $time_of_last_price{$stock_name})) and
				    ($last_row[$STOCK_LO_CNG] != 0) and ($stock_price < ($last_row[$STOCK_LOW]*(1+$low_threshold/100)))) {
					$recommendation = "Buy";
					$profit_price = sprintf("%.2f",$stock_price*(1+$profit_percentage/100));
					$stop_loss_price = sprintf("%.2f",$stock_price*(1-$stop_loss_percentage/100));
					$trade_trigerred = 1;
				} else {
					#Dont enter if we closed the position just in the last bar,
					#It is possible to see this since we sample twice.
					if ((!defined $close_trade_exit_time{$stock_name} or ($close_trade_exit_time{$stock_name} ne $time_of_last_price{$stock_name})) and
					    ($last_row[$STOCK_HI_CNG] != 0) and ($stock_price > ($last_row[$STOCK_HIGH]*(1-$high_threshold/100)))) {
						$profit_price = sprintf("%.2f",$stock_price*(1-$profit_percentage/100));
						$stop_loss_price = sprintf("%.2f",$stock_price*(1+$stop_loss_percentage/100));
						$trade_trigerred = 1;
					}
				}

				if ($trade_trigerred) {
					my $report_simulation = "";
					if ($simulation) {
						$report_simulation = "[Simulation Ignore]";
					}

					#Check with current open trades, if its same direction then ignore.
					if (!defined $open_trade_type{$stock_name} or $open_trade_type{$stock_name} ne $recommendation) {
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
							my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, ENTRY_TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME)
										VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, -200, 'OPEN', $stock_price, '$time_of_last_price{$stock_name}'));
							my $rv = $dbh->do($insert_stmt) or warn print "$insert_stmt\n";
						} else {
							#Close the old trade, and and new trade.
							my $profit_loss = 0;
							my $quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});
							my $closing_price = $stock_price;

							if ($open_trade_type{$stock_name} eq 'Buy') {
								$closing_price = ($open_trade_profit_price{$stock_name} <= $stock_price) ? $open_trade_profit_price{$stock_name} :
										    ($open_trade_stop_loss_price{$stock_name} >= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;
								$profit_loss = sprintf("%.2f",(($closing_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
							} else {
								$closing_price = ($open_trade_profit_price{$stock_name} >= $stock_price) ? $open_trade_profit_price{$stock_name} :
										    ($open_trade_stop_loss_price{$stock_name} <= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;
								$profit_loss = sprintf("%.2f",(($open_trade_entry_price{$stock_name} - $closing_price)*$quantity-$trade_commission));
							}
							my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = 'CLOSED',
									 CURRENT_PRICE = $stock_price, PROFIT_LOSS = $profit_loss,
									 EXIT_TIME = '$time_of_last_price{$stock_name}' WHERE ID == $open_trade_id{$stock_name};);
							my $my_ledger_rv = $dbh->do($update_stmt) or die $DBI::errstr;

							#Now add the new record
							my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, ENTRY_TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME)
										VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, -200.0, 'OPEN', $stock_price, '$time_of_last_price{$stock_name}'));

							#make sure update last_price code doesn't see this
							undef $open_trade_type{$stock_name};
						}

						$dbh->disconnect();
						$insert_id++;
					}
				}
			}

			#Update last_price and check if we need to close the trade due to profit or stop loss trigger.
			#Update only if last_row has valid data.
			if (defined $last_row[$STOCK_ID] and defined $open_trade_type{$stock_name}) {
				my $database = "high_volume_calls.db";
				my $dsn = "DBI:$driver:dbname=$database";
				my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, AutoCommit => 1 })
						      or die $DBI::errstr;
				my $trade_status = "OPEN";

				my $profit_loss = 0;
				my $quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});

				my $closing_price = $stock_price;

				#If low or high changed only then consider the low and high prices else use the last price.
				if ($last_row[$STOCK_HI_CNG] == 0.0) {
					$open_trade_high{$stock_name} = $stock_price;
				}

				if ($last_row[$STOCK_LO_CNG] == 0.0) {
					$open_trade_low{$stock_name} = $stock_price;
				}

				if ($open_trade_type{$stock_name} eq "Buy") {
					if (($open_trade_profit_price{$stock_name} <= $stock_price) or ($open_trade_profit_price{$stock_name} <= $open_trade_high{$stock_name}) or
					    ($open_trade_stop_loss_price{$stock_name} >= $stock_price)) {

						#Due to twice sampling freq, the same bar can be seen twice,
						#So check for last_row time if same as entry_time then dont close.
						if ($time_of_last_price{$stock_name} ne $open_trade_entry_time{$stock_name}) {
							$trade_status = "CLOSED";
						}
					}

					if ($trade_status eq "CLOSED") {
						$closing_price = ($open_trade_profit_price{$stock_name} <= $stock_price) ? $open_trade_profit_price{$stock_name} :
								 ($open_trade_profit_price{$stock_name} <= $open_trade_high{$stock_name}) ? $open_trade_profit_price{$stock_name} :
								 $stock_price;
					} else {
						$closing_price = $stock_price;
					}

					$profit_loss = sprintf("%.2f",(($closing_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
				} else {
					if (($open_trade_profit_price{$stock_name} >= $stock_price) or ($open_trade_profit_price{$stock_name} >= $open_trade_low{$stock_name}) or
					    ($open_trade_stop_loss_price{$stock_name} <= $stock_price)) {
						#Due to twice sampling freq, the same bar can be seen twice,
						#So check for last_row time if same as entry_time then dont close.
						if ($time_of_last_price{$stock_name} ne $open_trade_entry_time{$stock_name}) {
							$trade_status = "CLOSED";
						}
					}

					if ($trade_status eq "CLOSED") {
						$closing_price = ($open_trade_profit_price{$stock_name} >= $stock_price) ? $open_trade_profit_price{$stock_name} :
							 ($open_trade_profit_price{$stock_name} >= $open_trade_low{$stock_name}) ? $open_trade_profit_price{$stock_name} :
							 $stock_price;
					} else {
						$closing_price = $stock_price;
					}

					$profit_loss = sprintf("%.2f",(($open_trade_entry_price{$stock_name} - $closing_price)*$quantity-$trade_commission));
				}

				my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = '$trade_status',
							CURRENT_PRICE = $closing_price, PROFIT_LOSS = $profit_loss,
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
