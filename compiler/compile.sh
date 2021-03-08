(cd $PWD/../executor/spark && exec sbt package)
(sh $PWD/../executor/spark/run.sh GenerateCosts > out) # && cat out)
