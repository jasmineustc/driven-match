Pitt Cabs driver matching system
=================================

This project introduces the idea of stream processing using the most common use case 
for stream processing viz. processing sensor data in real time. In this project the 
"sensor" data is a stream of GPS location updates by a fictional private cab service,
Pitt Cabs. The first task involves joining 2 streams of data, the stream of driver 
locations and a stream of rider requests.

How to run
----------

Make your changes in src/main/java/pitt-cabs/DriverMatchTask.java and any other helper Java
files you wish to add. Implement the logic to join the 2 stream (DRIVER_LOCATIONS and EVENTS)
to provide an output stream of rider to driver matchings. You MUST use the fault tolerant
key-value store provided by Samza (refer to writeup) i.e no in memory hashmap etc.

How to build
----------

* `mvn clean package && rm -rf deploy/samza/* && mkdir -p deploy/samza && tar -xvf target/pitt_cabs-0.0.1-dist.tar.gz -C deploy/samza`
* `hadoop fs -rmr /pitt_cabs-0.0.1-dist.tar.gz`
* `hadoop fs -put target/pitt_cabs-0.0.1-dist.tar.gz /`

You can use kafka-console-consumer to inspect the output, e.g. the
`match-stream` topic.
