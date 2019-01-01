#! /usr/bin/perl

use lib qw( ..);
use DBI;
use DateTime qw(:all);
use POSIX;
use Time::HiRes qw(gettimeofday);

use strict;
use warnings;

my $simulation = 0;
my $send_email = 1;
my $email_list = "mimansoor\@gmail.com, lksingh74\@gmail.com, prathibha.chirag\@gmail.com";

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

#LAST_TIME field indexes
my $LAST_TIME_ID = 0;
my $LAST_TIME_NAME = 1;
my $LAST_TIME_OPEN = 2;
my $LAST_TIME_HIGH = 3;
my $LAST_TIME_LOW = 4;
my $LAST_TIME_LAST = 5;
my $LAST_TIME_PREVCLOSE = 6;
my $LAST_TIME_CHANGE = 7;
my $LAST_TIME_PERCHANGE = 8;
my $LAST_TIME_VOLUME = 9;
my $LAST_TIME_TIME = 12;
my $LAST_TIME_DATE = 13;

#View filed indexes
my $HIGH_VOL_STOCK_ID 			= 0;
my $HIGH_VOL_STOCK_NAME			= 1;
my $HIGH_VOL_STOCK_DATE			= 2;
my $HIGH_VOL_STOCK_ENTRY_TIME		= 3;
my $HIGH_VOL_STOCK_ENTRY_PRICE		= 4;
my $HIGH_VOL_STOCK_TRADE_TYPE 		= 5;
my $HIGH_VOL_STOCK_PROFIT_PRICE		= 6;
my $HIGH_VOL_STOCK_STOP_LOSS_PRICE 	= 7;
my $HIGH_VOL_STOCK_STOP_PROFIT		= 8;
my $HIGH_VOL_STOCK_PROFIT_LOSS		= 9;
my $HIGH_VOL_STOCK_TRADE_STATUS		= 10;
my $HIGH_VOL_STOCK_CURRENT_PRICE	= 11;
my $HIGH_VOL_STOCK_EXIT_TIME		= 12;
my $HIGH_VOL_STOCK_MAX_PROFIT		= 13;
my $HIGH_VOL_STOCK_MAX_LOSS		= 14;
my $HIGH_VOL_STOCK_QUANTITY		= 15;
my $HIGH_VOL_STOCK_STOP_LOSS_POINTS	= 16;
my $HIGH_VOL_STOCK_PROFIT_POINTS	= 17;

#FnO Market Lot table
my %fno_market_lot;

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
		#Start only after 9:20AM
		#Stop after 15:31PM
		if ($time_now->hour < 9 or $time_now->hour > 15) {
			$download = 0;
		} else {
			if ($time_now->hour == 9) {
				if ($time_now->minute < 20) {
					$download = 0;
				}
			} else {
				if ($time_now->hour == 15) {
					if ($time_now->minute > 31) {
						$download = 0;
					}
				}
			}
		}
	}

	return $download;
}

sub can_open_trade_time {
	my $time_now = shift(@_);
	#my $time_now = DateTime->now( time_zone => 'local' );
	$time_now =~ /(..):(..):(..)/;
	my $hour = $1;
	my $minute = $2;
	my $second = $3;
	my $can_open_trade = 1;

	#Dont open trades before 9:30AM OR at or after 2:45PM
	if ($hour <= 9 or $hour >= 14) {
		if(($hour == 9) && ($minute >= 30)) {
			$can_open_trade = 1;
		} else {
			if (($hour == 14) && ($minute <= 45)) {
				$can_open_trade = 1;
			} else {
				$can_open_trade = 0;
			}
		}
	}

	return $can_open_trade;
}

sub can_close_trade_time {
	my $time_now = shift(@_);
	#my $time_now = DateTime->now( time_zone => 'local' );
	$time_now =~ /(..):(..):(..)/;
	my $hour = $1;
	my $minute = $2;
	my $second = $3;
	my $can_close_trade = 0;

	#Close trades before 9:30AM or after 3:15PM
	if ($hour <= 9 or $hour >= 15) {
		if ($hour <= 9) {
			if ($minute < 30) {
				$can_close_trade = 1;
			}
		} else {
			if ($minute > 15) {
				$can_close_trade = 1;
			}
		}
	}

	return $can_close_trade;
}

sub load_fno_market_lot {
	my $fno_market_lot_file = "fo_mktlots.csv";

	unless(open (FILE, "< $fno_market_lot_file")) {
		print "Could not open!\n";
		exit;
	}

	my $FO_NAME = 0;
	my $FO_SYMBOL = 1;
	my $FO_NEAR_MONTH_LOT_SIZE = 2;
	#Remove the header line
	my $line = <FILE>;
	while (my $line = <FILE>) {
		my @tokens = split/,/, $line;
		my $symbol = $tokens[$FO_SYMBOL];

		#skip line that has Symbol as symbol name
		if ($symbol eq 'Symbol') {
			next;
		}

		if ($symbol eq 'NIFTY') {
			$symbol = "NIFTY_50";
		}

		$fno_market_lot{$symbol} = $tokens[$FO_NEAR_MONTH_LOT_SIZE];
	}

	close(FILE);
}

#Main Starts Here
STDOUT->autoflush(1);

my $driver   = "SQLite";
my $database_vol = "check_volume.db";
my $dsn = "DBI:$driver:dbname=$database_vol";
my $userid = "";
my $password = "";

my $repeat_always = 1;
my $delay_in_seconds = $simulation == 1? 5: 30;

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
my %time_of_last_price;
my %date_of_last_price;
my $lot_size_in_cash = 825000;
my $trade_commission = 250;
my $stop_loss_percentage = 0.70;
my $profit_percentage = $stop_loss_percentage*2.00;
my $cash_profit_target = 40000;
my $cash_loss_target = -40000;
my $low_threshold = 0.1;
my $high_threshold = 0.1;
my $buy_change_threshold = -20.00;
my $sell_change_threshold = 20.00;
my $profit_dec_rate_per = 0.008;
my $buy_per_threshold = 20.0;
my $sell_per_threshold = -20.0;

#After making 1 : 1 profit take atleast some money home
my $take_home_threshold = $stop_loss_percentage;
my $take_home_threshold_cash = 1500;

#Load Market Lots from file
load_fno_market_lot();

#have a counter for loop
my $counter = 1;
my $dec_n = 4;

while ($repeat_always) {
	my ($start_time, $utime) = gettimeofday();

	#Get some unique id value
	my $insert_id = $start_time*1000+$utime%300;

	if (check_for_download_time()) {
		if ($simulation) {
			system("cp realtime_copy.db $database_vol");
		} else {
			system("cp 01realtime_data.db $database_vol");
		}

		#Get the lastest price cached for all stocks.
		my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
				      or die $DBI::errstr;

		my $sth = $dbh->prepare('SELECT * FROM LAST_TIME')
			or die "Couldn't prepare statement: " . $dbh->errstr;
		$sth->execute();
		while (@data = $sth->fetchrow_array()) {
		 $high_price{$data[$LAST_TIME_NAME]} = $data[$LAST_TIME_HIGH];
		 $low_price{$data[$LAST_TIME_NAME]} = $data[$LAST_TIME_LOW];
		 $cur_price{$data[$LAST_TIME_NAME]} = $data[$LAST_TIME_LAST];
		 $time_of_last_price{$data[$LAST_TIME_NAME]} = $data[$LAST_TIME_TIME];
		 $date_of_last_price{$data[$LAST_TIME_NAME]} = $data[$LAST_TIME_DATE];
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
		my %open_trade_max_profit;
		my %open_trade_max_loss;
		my %open_trade_stop_loss_points;
		my %open_trade_profit_points;

		#Just want to create a scope for local variables.
		{
			my $high_vol_db = "high_volume_calls.db";
			my $dsn = "DBI:$driver:dbname=$high_vol_db";
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;
			my $stmt = qq(SELECT * FROM high_volume_calls_v2 WHERE TRADE_STATUS='OPEN');
			my $sth = $dbh->prepare($stmt) or die $DBI::errstr;
			$sth->execute();
			while (my @data = $sth->fetchrow_array()) {
				$open_trade_id{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_ID];
				$open_trade_entry_time{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_ENTRY_TIME];
				$open_trade_entry_price{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_ENTRY_PRICE];
				$open_trade_type{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_TRADE_TYPE];
				$open_trade_profit_price{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_PROFIT_PRICE];
				$open_trade_stop_loss_price{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_STOP_LOSS_PRICE];
				$open_trade_max_profit{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_MAX_PROFIT];
				$open_trade_max_loss{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_MAX_LOSS];
				$open_trade_stop_loss_points{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_STOP_LOSS_POINTS];
				$open_trade_profit_points{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_PROFIT_POINTS];

				#Add the high,low and last, price into ledger
				$open_trade_current_price{$data[$HIGH_VOL_STOCK_NAME]} = $cur_price{$data[$HIGH_VOL_STOCK_NAME]};
				$open_trade_low{$data[$HIGH_VOL_STOCK_NAME]} = $low_price{$data[$HIGH_VOL_STOCK_NAME]};
				$open_trade_high{$data[$HIGH_VOL_STOCK_NAME]} = $high_price{$data[$HIGH_VOL_STOCK_NAME]};
			}
			$sth->finish();
			$dbh->disconnect();
		}

		#Cache into memory Close Trade Ledger for this stock.
		my %close_trade_exit_time;

		#Just want to create a scope for local variables.
		{
			my $high_vol_db = "high_volume_calls.db";
			my $dsn = "DBI:$driver:dbname=$high_vol_db";
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;
			my $stmt = qq(SELECT * FROM high_volume_calls_v2 WHERE TRADE_STATUS='CLOSED' order by exit_time asc);
			my $sth = $dbh->prepare($stmt) or die $DBI::errstr;
			$sth->execute();

			#Let it overwrite the exit_time so we get the last trade exit time only.
			while (my @data = $sth->fetchrow_array()) {
				$close_trade_exit_time{$data[$HIGH_VOL_STOCK_NAME]} = $data[$HIGH_VOL_STOCK_EXIT_TIME];
			}
			$sth->finish();
			$dbh->disconnect();
		}

		#For each stock check if trade triggered
		foreach my $stock_name (sort keys %cur_price)
		{
			#local alias for last price of this stock.
			my $stock_price = $cur_price{$stock_name};
			my $quantity = $fno_market_lot{$stock_name};

			#Get the last row to see if trade triggered.
			my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1 })
					      or die $DBI::errstr;

			#my $stmt = qq(SELECT * FROM $stock_name WHERE ID=\(SELECT MAX(ID) FROM $stock_name\));
			my $stmt = qq(SELECT * FROM \"$stock_name\" LIMIT 2 OFFSET \(SELECT COUNT(*) FROM \"$stock_name\"\)-2);

			$sth = $dbh->prepare($stmt)
				or die "Couldn't prepare statement: " . $dbh->errstr;
			$sth->execute();
			my @lastb1_row = $sth->fetchrow_array();
			my @last_row = $sth->fetchrow_array();
			$sth->finish();
			$dbh->disconnect();
		
			#This is high_volume == 1, if trade triggered.
			#Buy/Sell Algo
			#	Buy: When you find a Non-zero low_change and %change < n%  with high_volumes
			#	Buy: When you find a Non-zero high_change and %change > n%  with high_volumes
			#	Sell: When you find a Non-zero high_change and %change > n% with high_volumes
			#	Sell: When you find a Non-zero low_change and %change < n% with high_volumes
			if ((defined $last_row[$STOCK_HIGH_VOL]) and (defined $lastb1_row[$STOCK_HIGH_VOL]) and
				($lastb1_row[$STOCK_HIGH_VOL] == 1) and ($last_row[$STOCK_PERCNG] < $buy_per_threshold) and
				($lastb1_row[$STOCK_LO_CNG] != 0 or $lastb1_row[$STOCK_HI_CNG] != 0) and
				can_open_trade_time($last_row[$STOCK_TIME])) {
				my $recommendation;

				my $stop_loss_price = 0.0;
				my $profit_price = 0.0;
				my $stop_loss_points = 0.0;
				my $profit_points = 0.0;

				my $trade_trigerred = 0;
				#Dont enter if we closed the position just in the last bar,
				#It is possible to see this since we sample twice.
				if ((!defined $close_trade_exit_time{$stock_name} or
				     ($close_trade_exit_time{$stock_name} ne $time_of_last_price{$stock_name})) and
				    ($lastb1_row[$STOCK_HI_CNG] != 0) and
				    ($lastb1_row[$STOCK_LAST] > ($lastb1_row[$STOCK_HIGH]*(1-$high_threshold/100))) and
				    ($stock_price < $lastb1_row[$STOCK_LAST])) {
					if (($last_row[$STOCK_PERCNG] > $sell_change_threshold)) {
						$recommendation = "Short_Sell";
					} else {
						#$recommendation = "Buy";
						$recommendation = "Short_Sell";
					}

					$trade_trigerred = 1;
				} else {
					#Dont enter if we closed the position just in the last bar,
					#It is possible to see this since we sample twice.
					if ((!defined $close_trade_exit_time{$stock_name} or
					     ($close_trade_exit_time{$stock_name} ne $time_of_last_price{$stock_name})) and
					     ($lastb1_row[$STOCK_LO_CNG] != 0) and ($last_row[$STOCK_PERCNG] > $sell_per_threshold) and
					     ($lastb1_row[$STOCK_LAST] < ($lastb1_row[$STOCK_LOW]*(1+$low_threshold/100))) and
					     ($stock_price > $lastb1_row[$STOCK_LAST])) {
						if (($last_row[$STOCK_PERCNG] < $buy_change_threshold)) {
							$recommendation = "Buy";
						} else {
							#$recommendation = "Short_Sell";
							$recommendation = "Buy";
						}

						$trade_trigerred = 1;
					}
				}

				if ($trade_trigerred) {
					my $report_simulation = "";
					if ($simulation) {
						$report_simulation = "[Simulation Ignore]";
					}

					if ($recommendation eq "Buy") {
						$profit_price = sprintf("%.02f",$stock_price*(1+$profit_percentage/100.0));
						$stop_loss_price = sprintf("%.02f",$stock_price*(1-$stop_loss_percentage/100.0));
						$stop_loss_points = sprintf("%.02f", ($stock_price - $stop_loss_price));
						$profit_points = sprintf("%.02f", ($profit_price - $stock_price));
					} else {
						$profit_price = sprintf("%.02f",$stock_price*(1-$profit_percentage/100.0));
						$stop_loss_price = sprintf("%.02f",$stock_price*(1+$stop_loss_percentage/100.0));
						$stop_loss_points = sprintf("%.02f", ($stop_loss_price - $stock_price));
						$profit_points = sprintf("%.02f", ($stock_price - $profit_price));
					}

					#Check with current open trades, if its same direction then ignore.
					if (!defined $open_trade_type{$stock_name} or $open_trade_type{$stock_name} ne $recommendation) {
						#Check if its new trade recommendation.
						my $new_trade = 0;
						if (!defined $open_trade_type{$stock_name}) {
							$new_trade = 1;
						}

						#Now store it in a DB with Buy, take profit and stop loss values
						my $database = "high_volume_calls.db";
						my $dsn = "DBI:$driver:dbname=$database";
						my $dbh = DBI->connect($dsn, $userid, $password, { RaiseError => 1, AutoCommit => 1 })
								      or die $DBI::errstr;

						#If New Trade just Insert Row
						if ($new_trade) {
							#Send email notifications if enabled
							my $email_recommendation = $recommendation;
							my $email_cmd = qq(-s "Fourways Profit: $report_simulation $time_of_last_price{$stock_name}: $email_recommendation $stock_name \($stock_price\) Target: $profit_price StopLoss: $stop_loss_price" $email_list < /dev/null);
							my $email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;

							my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, ENTRY_TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, STOP_PROFIT, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME, MAX_PROFIT, MAX_LOSS, QUANTITY, STOP_LOSS_POINTS, PROFIT_POINTS)
										VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, ($trade_commission*-1.0), ($trade_commission*-1.0), 'OPEN', $stock_price, '$time_of_last_price{$stock_name}', ($trade_commission*-1.0), ($trade_commission*-1.0), $quantity, $stop_loss_points, $profit_points));
							my $rv = $dbh->do($insert_stmt) or warn print "$insert_stmt\n";
						} else {
							#Close the old trade, and open new trade.
							my $profit_loss = 0.0;
							my $stop_profit = 0.0;

							$quantity = $fno_market_lot{$stock_name};
							if ($quantity eq '') {
								$quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});
							}
							my $closing_price = $stock_price;

							if ($open_trade_type{$stock_name} eq 'Buy') {
								$closing_price = ($open_trade_profit_price{$stock_name} <= $stock_price) ? $open_trade_profit_price{$stock_name} :
										    ($open_trade_stop_loss_price{$stock_name} >= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;
								$profit_loss = sprintf("%.02f",(($closing_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
								$stop_profit = sprintf("%.02f",(($open_trade_stop_loss_price{$stock_name} - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
							} else {
								$closing_price = ($open_trade_profit_price{$stock_name} >= $stock_price) ? $open_trade_profit_price{$stock_name} :
										    ($open_trade_stop_loss_price{$stock_name} <= $stock_price) ? $open_trade_stop_loss_price{$stock_name} : $stock_price;
								$profit_loss = sprintf("%.02f",(($open_trade_entry_price{$stock_name} - $closing_price)*$quantity-$trade_commission));
								$stop_profit = sprintf("%.02f",(($open_trade_entry_price{$stock_name} - $open_trade_stop_loss_price{$stock_name})*$quantity-$trade_commission));
							}
							$stop_profit = sprintf("%.02f", $stop_profit);
							$profit_loss = sprintf("%.02f", $profit_loss);

							my $max_profit = $open_trade_max_profit{$stock_name};
							if ($profit_loss > $max_profit) {
								$max_profit = $profit_loss;
							}

							my $max_loss = $open_trade_max_loss{$stock_name};
							if ($profit_loss < $max_loss) {
								$max_loss = $profit_loss;
							}

							my $profit_target = $open_trade_profit_price{$stock_name};
							my $stop_loss = $open_trade_stop_loss_price{$stock_name};
							my $profit_target_u = sprintf("%.02f", (int($profit_target*100)-(($profit_target*100)%5))/100.0);
							my $stop_profit_u = sprintf("%.02f", (int($stop_profit*100)-(($stop_profit*100)%5))/100.0);
							my $profit_loss_u = sprintf("%.02f", (int($profit_loss*100)-(($profit_loss*100)%5))/100.0);
							my $stop_loss_u = sprintf("%.02f", (int($stop_loss*100)-(($stop_loss*100)%5))/100.0);

							#If its not a new trade then its Cover and Reverse.
							#Close the Old trade
							my $email_recommendation = "CLOSED";
							my $email_cmd = qq(-s "Fourways Profit: $report_simulation $time_of_last_price{$stock_name}: $email_recommendation $stock_name \($stock_price\) Porfit: $profit_loss_u Max_Drawdown: $max_loss Max_Profit: $max_profit" $email_list < /dev/null);
							my $email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;

							my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = 'CLOSED',
									 CURRENT_PRICE = $closing_price, PROFIT_PRICE = $profit_target_u,
									 STOP_LOSS_PRICE = $stop_loss_u, STOP_PROFIT = $stop_profit_u,
									 PROFIT_LOSS = $profit_loss_u, MAX_PROFIT = $max_profit, MAX_LOSS = $max_loss,
									 EXIT_TIME = '$time_of_last_price{$stock_name}' WHERE ID == $open_trade_id{$stock_name};);
							my $my_ledger_rv = $dbh->do($update_stmt) or die $DBI::errstr;

							#Now Open new position
							$email_recommendation = $recommendation;
							$email_cmd = qq(-s "Fourways Profit: $report_simulation $time_of_last_price{$stock_name}: $email_recommendation $stock_name \($stock_price\) Target: $profit_price StopLoss: $stop_loss_price" $email_list < /dev/null);
							$email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;
							#Now add the new record
							my $insert_stmt = qq(INSERT INTO high_volume_calls_v2 (ID, NAME, DATE, ENTRY_TIME, ENTRY_PRICE, TRADE_TYPE, PROFIT_PRICE, STOP_LOSS_PRICE, STOP_PROFIT, PROFIT_LOSS, TRADE_STATUS, CURRENT_PRICE, EXIT_TIME, MAX_PROFIT, MAX_LOSS, QUANTITY, STOP_LOSS_POINTS, PROFIT_POINTS)
										VALUES ($insert_id, '$stock_name', '$date_of_last_price{$stock_name}', '$time_of_last_price{$stock_name}', $stock_price, '$recommendation', $profit_price, $stop_loss_price, ($trade_commission*-1.0), ($trade_commission*-1.0), 'OPEN', $stock_price, '$time_of_last_price{$stock_name}', ($trade_commission*-1.0), ($trade_commission*-1.0), $quantity, $stop_loss_points, $profit_points));

							my $rv = $dbh->do($insert_stmt) or warn print "$insert_stmt\n";

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

				my $profit_loss = 0.0;
				my $stop_profit = 0.0;
				my $profit_points = $open_trade_profit_points{$stock_name};
				my $stop_loss_points = $open_trade_stop_loss_points{$stock_name};

				#my $quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});
				$quantity = $fno_market_lot{$stock_name};
				if ($quantity eq '') {
					$quantity = floor($lot_size_in_cash/$open_trade_entry_price{$stock_name});
				}

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

					$closing_price = ($trade_status eq "CLOSED") ? (($open_trade_profit_price{$stock_name} <= $stock_price) ?
							 $open_trade_profit_price{$stock_name} : ($open_trade_profit_price{$stock_name} <= $open_trade_high{$stock_name}) ?
							 $open_trade_profit_price{$stock_name} : $open_trade_stop_loss_price{$stock_name}) : $stock_price;

					$profit_loss = sprintf("%.02f",(($closing_price - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
					$stop_profit = sprintf("%.02f",(($open_trade_stop_loss_price{$stock_name} - $open_trade_entry_price{$stock_name})*$quantity-$trade_commission));
				} else {
					if (($open_trade_profit_price{$stock_name} >= $stock_price) or ($open_trade_profit_price{$stock_name} >= $open_trade_low{$stock_name}) or
					    ($open_trade_stop_loss_price{$stock_name} <= $stock_price)) {
						#Due to twice sampling freq, the same bar can be seen twice,
						#So check for last_row time if same as entry_time then dont close.
						if ($time_of_last_price{$stock_name} ne $open_trade_entry_time{$stock_name}) {
							$trade_status = "CLOSED";
						}
					}

					$closing_price = ($trade_status eq "CLOSED") ? (($open_trade_profit_price{$stock_name} >= $stock_price) ?
							 $open_trade_profit_price{$stock_name} : ($open_trade_profit_price{$stock_name} >= $open_trade_low{$stock_name}) ?
							 $open_trade_profit_price{$stock_name} : $open_trade_stop_loss_price{$stock_name}) : $stock_price;

					$profit_loss = sprintf("%.02f",(($open_trade_entry_price{$stock_name} - $closing_price)*$quantity-$trade_commission));
					$stop_profit = sprintf("%.02f",(($open_trade_entry_price{$stock_name} - $open_trade_stop_loss_price{$stock_name})*$quantity-$trade_commission));
				}

				#Close position if we met minimum profit.
				if ($profit_loss >= $cash_profit_target) {
					$trade_status = "CLOSED";
					$profit_loss = $cash_profit_target;
				}

				#Close position if we met maximum loss.
				if ($profit_loss <= $cash_loss_target) {
					$trade_status = "CLOSED";
					$profit_loss = $cash_loss_target;
				}

				#Close positions if close time reached.
				if (can_close_trade_time($time_of_last_price{$stock_name})) {
					$trade_status = "CLOSED";
				}

				#Update trailing stop loss if price is moving up
				#Update profit @ profit_dec_rate_per
				my $profit_target = $open_trade_profit_price{$stock_name};
				my $stop_loss = $open_trade_stop_loss_price{$stock_name};
				if ($trade_status ne "CLOSED") {
					if ($open_trade_type{$stock_name} eq "Buy") {
						#Always trail by stop_loss_points
						my $st = sprintf("%.4f", ($closing_price-$stop_loss_points));

						#check if profit_loss > threshold profit
						if ($profit_loss >= $take_home_threshold_cash) {
							my $cash_threshold_pr = $open_trade_entry_price{$stock_name} + ($take_home_threshold_cash / $quantity);
							if ($st < $cash_threshold_pr) {
								$st = $cash_threshold_pr;
							}
						}

						#push the stop loss up
						if ($stop_loss < $st) {
							$stop_loss = $st;
						}

						#After making profit at least X*$take_home_threshold then take atleast $take_home_threshold home
						my $take_home_price = $open_trade_entry_price{$stock_name} * (1+($take_home_threshold*1.1)/100.0);
						if ($closing_price >= $take_home_price) {
							$st = $open_trade_entry_price{$stock_name} * (1+$take_home_threshold/100.0);
						}

						#push the stop loss up
						if ($stop_loss < $st) {
							$stop_loss = $st;
						}

						#Only reduce profit_target after dec_n loops
						if (($counter % $dec_n) == 0) {
							$profit_target *= (1 - $profit_dec_rate_per/100.0);
							$profit_points = sprintf("%.02f", ($profit_target - $stock_price));
						}

					} else {
						#Always trail by stop_loss_points
						my $st = sprintf("%.4f", ($closing_price+$stop_loss_points));

						#check if profit_loss > threshold profit
						if ($profit_loss >= $take_home_threshold_cash) {
							my $cash_threshold_pr = $open_trade_entry_price{$stock_name} - ($take_home_threshold_cash / $quantity);
							if ($st > $cash_threshold_pr) {
								$st = $cash_threshold_pr;
							}
						}

						#pull the stop_loss down
						if ($stop_loss > $st) {
							$stop_loss = $st;
						}

						#After making profit at least X*$take_home_threshold then take atleast $take_home_threshold home
						my $take_home_price = $open_trade_entry_price{$stock_name} * (1-($take_home_threshold*1.1)/100.0);
						if ($closing_price <= $take_home_price) {
							$st = $open_trade_entry_price{$stock_name} * (1-$take_home_threshold/100.0);
						}

						#pull the stop_loss down
						if ($stop_loss > $st) {
							$stop_loss = $st;
						}

						#Only reduce profit_target after dec_n loops
						if (($counter % $dec_n) == 0) {
							$profit_target *= (1 + $profit_dec_rate_per/100.0);
							$profit_points = sprintf("%.02f", ($stock_price - $profit_target));
						}
					}
				}

				my $profit_target_u = sprintf("%.02f", (int($profit_target*100)-(($profit_target*100)%5))/100.0);
				my $stop_profit_u = sprintf("%.02f", (int($stop_profit*100)-(($stop_profit*100)%5))/100.0);
				my $profit_loss_u = sprintf("%.02f", (int($profit_loss*100)-(($profit_loss*100)%5))/100.0);
				my $stop_loss_u = sprintf("%.02f", (int($stop_loss*100)-(($stop_loss*100)%5))/100.0);
				my $profit_points_u = sprintf("%.02f", (int($profit_points*100)-(($profit_points*100)%5))/100.0);
				my $stop_loss_points_u = sprintf("%.02f", (int($stop_loss_points*100)-(($stop_loss_points*100)%5))/100.0);

				my $max_profit = $open_trade_max_profit{$stock_name};
				if ($profit_loss >= $max_profit) {
					$max_profit = $profit_loss_u;
				}

				my $max_loss = $open_trade_max_loss{$stock_name};
				if ($profit_loss <= $max_loss) {
					$max_loss = $profit_loss_u;
				}

				if ($trade_status eq "CLOSED") {
					#Send email alert that the trade got closed.
					my $report_simulation = "";
					if ($simulation) {
						$report_simulation = "[Simulation Ignore]";
					}

					my $email_recommendation = "CLOSED";
					my $email_cmd = qq(-s "Fourways Profit: $report_simulation $time_of_last_price{$stock_name}: $email_recommendation $stock_name \($stock_price\) Profit: $profit_loss_u Max_Drawdown: $max_loss Max_Profit: $max_profit" $email_list < /dev/null);
					my $email_sent = $send_email == 1? system("$email_program $email_cmd") : 0;
				}

				my $update_stmt = qq(UPDATE high_volume_calls_v2 set TRADE_STATUS = '$trade_status',
							CURRENT_PRICE = $closing_price, PROFIT_PRICE = $profit_target_u,
							STOP_LOSS_PRICE = $stop_loss_u, STOP_PROFIT = $stop_profit_u,
							PROFIT_LOSS = $profit_loss_u, MAX_PROFIT = $max_profit, MAX_LOSS = $max_loss,
							EXIT_TIME = '$time_of_last_price{$stock_name}', STOP_LOSS_POINTS = $stop_loss_points_u,
							PROFIT_POINTS = $profit_points_u WHERE ID == $open_trade_id{$stock_name};);
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
	sleep $sleep_time;
	$counter += 1;
}
