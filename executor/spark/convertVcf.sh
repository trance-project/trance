spark-submit --class sparkutils.loader.App  --master "local[1]"  --executor-cores 1  --num-executors 1  --executor-memory 16G  --jars libs/hadoop-bam-7.8.0.jar,libs/htsjdk-2.9.1.jar  target/scala-2.12/sparkutils_2.12-0.1.jar >> out