# spark-cassandra-java

Reference: http://www.datastax.com/dev/blog/accessing-cassandra-from-spark-in-java


### Build
##### gradle clean build

### Upload files to Spark server
##### create spark-cassandra-java/ directory in spark server
mkdir spark-wordcount-java
##### upload build/libs/spark-wordcount-java-1.0.jar

### run spark-submit
##### go to spark-cassandra-java/ in spark server
cd spark-wordcount-java
##### run spark-submit
spark-submit --class dannyk.project.JavaDemo --jars build/libs/spark-wordcount-java-1.0.jar \
--master {master} spark-wordcount-java-1.0.jar {spark master} {input file name}


spark-submit --class dannyk.project.WordCount --jars build/libs/spark-wordcount-java-1.0.jar \
--master local[4] spark-wordcount-java-1.0.jar local build.gradle