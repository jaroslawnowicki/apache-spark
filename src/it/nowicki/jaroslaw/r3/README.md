Source get from site : http://www-etud.iro.umontreal.ca/~bergstrj/audioscrobbler_data.html

Download file:  profiledata_06-May-2005.tar.gz

scalac -classpath "/home/jarek/Programy/spark-2.1.1-bin-hadoop2.7/jars/*" src/it/nowicki/jaroslaw/r3/RecommenderSystem.scala -d R3.jar

spark-submit --class it.nowicki.jaroslaw.r3.RecommenderSystem --master local[*]  --driver-memory 12g R3.jar /home/jarek/Pobrane/profiledata_06-May-2005/ (1 | 2 | 3 | 4)

Progress work maybe look for address: http://localhost:4040/jobs/

Results 1:

RecommenderSystem$: WrappedArray(Aerosmith (unplugged)) -> WrappedArray(Aerosmith)

Results 2:

50 Cent
Snoop Dogg
Dr. Dre
2Pac
The Game


Results 3:

((50,1.0,40.0),0.9777857277171028)

((10,1.0,40.0),0.9768882278160927)

((50,1.0E-4,40.0),0.9764796146476783)

((10,1.0E-4,40.0),0.9755744963323143)

((10,1.0,1.0),0.9694939031761849)

((50,1.0,1.0),0.9673211212524118)

((10,1.0E-4,1.0),0.9650519211633238)

((50,1.0E-4,1.0),0.9546093305574926)



Results 4:

2188602 -> 1019715, 1007902, 6632684, 1005345, 1006894

2402725 -> 2814, 930, 1007614, 1037970, 1026440

2277899 -> 1034635, 1000113, 4267, 976, 1205

2003742 -> 1022207, 1005493, 1004422, 1008953, 1005034

1010282 -> 1233389, 1009489, 1233239, 1233715, 1003853
(... x 100)
