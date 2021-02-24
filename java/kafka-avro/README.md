
## Build

These java examples are build using `gradle`.  
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

## start kafka

```
% docker-compose up -d
Starting kafka-tools ... done
Starting zookeeper   ... done
Starting broker      ... done
Starting schema-registry ... done
Starting control-center  ... done
```

Visit http://127.0.0.1:9021 to see the kafka health dashboard.  From the dashboard you can see assess the health f the kefka cluster and see any active topics.


