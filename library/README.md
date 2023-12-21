# TraNCE Programming Library

### This programming library is currently a work in progress.

* To run and use the library while it is in development, follow the Instructions section for local installation guide
* Currently tested and working with Java 11, Scala 2.12 & Spark 3.3.2

### Instructions:
```git
git clone -b trance_library https://github.com/trance-project/trance.git
```

```bash
cd trance/compiler 
sbt clean publishLocal
```
```bash
cd trance/library
sbt clean publishLocal
```

Add the library as a dependency in the build.sbt of your project:
```sbt
libraryDependencies += "trance" % "library_2.12" % "0.1"
```


### Usage:

```scala

import uk.ac.ox.cs.trance.Wrapper.DataFrameImplicit

val df: DataFrame = {
  val data: Seq[(String, String)] = Seq(("1", "20"), ("2", "90"), ("3", "100"), ("4", "10"))
  import spark.implicits._
  data.toDF("id", "users")
}

// The wrap function call lifts the Dataframe into the library's WrappedDataframe type  
val wrappedDf: WrappedDataframe = df.wrap() 

// From there it's possible to perform operations as normal (See supported operations section) 
val selected: WrappedDataframe = df.select(df("id"))

// To return to Dataframe there is the leaveNRC() function
val result: Dataframe = selected.leaveNRC()

```

### Supported Operations

* Select
* Union
* Join
* Drop Duplicates
* Filter
* GroupBy -> Sum
