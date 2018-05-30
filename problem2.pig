REGISTER '/home/acadgild/install/pig/pig-0.16.0/lib/piggybank.jar';
delayed_flight = load 'DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
cancelled_flight = foreach delayed_flight generate (int)$2 as month,(int)$10 as flight_num,(int)$22 as cancelled,(chararray)$23 as cancel_code;
weather_cancellation = filter cancelled_flight by cancelled == 1 AND cancel_code =='B';
wc_month = group weather_cancellation by month;
count_bymonth = foreach wc_month generate group, COUNT(weather_cancellation.cancelled);
desc_count = order count_bymonth by $1 DESC;
Result = limit desc_count 1;
dump Result;
