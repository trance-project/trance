## Shredder

To compile:
```
sbt compile
```

To run native scala:
```
sbt run
```

To run spark:
```
sbt package
spark-submit --class shredding.calc.App --master "local[*]" target/scala-2.12/shredder_2.12-0.1.jar
```
