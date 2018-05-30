REGISTER '/home/acadgild/install/pig/pig-0.16.0/lib/piggybank.jar';
delayed_flight = load 'DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
diverted_flight = FOREACH delayed_flight GENERATE (chararray)$17 as origin, (chararray)$18 as dest, (int)$24 as diversion;
filter_diverted_flight = FILTER diverted_flight BY (origin is not null) AND (dest is not null) AND (diversion == 1);
group_route = GROUP filter_diverted_flight by (origin,dest);
list_route = FOREACH group_route generate group, COUNT(filter_diverted_flight.diversion);
list_route_desc = ORDER list_route BY $1 DESC;
Result = limit list_route_desc 10;
dump Result;

