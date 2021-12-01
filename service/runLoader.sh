spark-submit --class sparkutils.loader.App \
      --master "local[*]" \
      --driver-memory 16G \
      --conf "spark.sql.warehouse.dir=/Users/jac/code/trance/service/" \
      --conf "spark.hadoop.hive.metastore.warehouse.dir=/Users/jac/code/trance/service/" lib/sparkutils_2.12-0.1.jar
