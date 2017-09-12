# nycHotspots
This application computes the Getis-Ord score on a sliding time window, considered 3 days at a time on the NYC Yellow Taxi dataset to identify the top 50 Taxi hotspots in New York City.

EXECUTION INSTRUCTIONS

Execute the following command to run the code -
./bin/spark-submit --class ddsPhase3 [path to jar file]/MARS_phase3.jar [path to input] [path to output]

Main class - computeHotspots.scala

Input - csv file containing the NYC Taxi Trip dataset with columns "tpep_pickup_datetime", "pickup_longitude", "pickup_latitude"
Output - csv file containing the top 50 hotspots sorted in descending order of zscore. CSV file structure - latitude, longitude, timestep, Gets-Ord statistic 

