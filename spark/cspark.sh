spark-submit --class sprkloader.App \
  --master "local[*]" \
  --driver-class-path target/scala-2.12/sprkloader_2.12-0.1.jar target/scala-2.12/sprkloader_2.12-0.1.jar
