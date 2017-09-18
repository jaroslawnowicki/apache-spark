Download data for example from: http://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz

scalac -classpath "/home/jarek/Programy/spark-2.1.1-bin-hadoop2.7/jars/*" src/it/nowicki/jaroslaw/r4/RunRDF.scala -d R4.jar -deprecation

spark-submit --class it.nowicki.jaroslaw.r4.RunRDF --master local[*]  --driver-memory 12g R4.jar /home/jarek/Pobrane/covtype.data/covtype.data 1


Results 1:

(0.687080911935628,0.6566505269154087)

(0.7110438256960919,0.8044852551651068)

(0.651238139319602,0.7728645976380115)

(0.5481171548117155,0.44107744107744107)

(0.0,0.0)

(0.8115942028985508,0.0326530612244898)

(0.7158999192897498,0.4401985111662531)


Results 2:

0.3770795067786822

Results 3:

0.9526862190272255


Results 4:

0.9445539419087137

Results 5:

0.9659777095338326

4.0

