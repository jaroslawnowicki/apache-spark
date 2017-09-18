# spark
Spark

Remember add library spark.core to your IDE

scalac -classpath "/home/jarek/Programy/spark-2.1.1-bin-hadoop2.7/jars/*" src/com/example/MainExample.scala -d test.jar

spark-submit --class MainExample --master local test.jar

source get from site: http://bit.ly/1Aoywaq

production: spark-shell --master yarn-client

dev:

spark-shell --master local[*]

used all threads in computer

--driver-memory 2g - for dev

like commands:

:help

:history or :h?


