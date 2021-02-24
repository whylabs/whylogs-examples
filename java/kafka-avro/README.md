
## Environment Setup

Clone the whylogs-examples repo from GitHub and work in the java/kafka-avro/ subdirectory

Use docker-compose to bring up a basic Kafka cluster and control dashboard.
```
% docker-compose up -d
Creating network "kafka-avro_default" with the default driver
Creating zookeeper   ... done
Creating kafka-tools ... done
Creating broker      ... done
Creating schema-registry ... done
Creating control-center  ... done
```

Visit http://127.0.0.1:9021 to see the kafka health dashboard.  From the dashboard you can see assess the health f the kefka cluster and see any active topics.

## Schema Definition

These java examples are built using `gradle`.  
If you already have your Java environment set up, you should not need to install anything more to build these examples.

```
./gradlew build
```

## If you get an error...

`Could not initialize class org.codehaus.groovy.reflection.ReflectionCache`

Try upgrading your gradle installation.
```
gradle wrapper --gradle-version 6.3
```
I upgraded from 6.1 to 6.3. IntelliJ warns me that Gradle 6.3 is incompatible with 
amazon-corretto-15.jdk but I have not seen ill-effects yet.

## Run the demos

There are two separate demos that carry out the duties of a Kafka Producer and Consumer.
The Producer and Consumer demos can be run sequentially in the same window as the events written are persistent in Kafka.

```
% gradle producer

> Task :producer
opening lending_club_1000.csv
Sent event...
Sent event...
Sent event...
Sent event...
```

```
% gradle consumer

> Task :consumer
Read 500 records
Read 444 records
Read 56 records
Read 0 records
Received 1000 events
Writing profile to profile_2021.bin
```




