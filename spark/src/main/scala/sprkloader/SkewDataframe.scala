package sprkloader

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SkewDataset{

  implicit class DatasetOps[T: Encoder: ClassTag](left: Dataset[T]) extends Serializable {

    def empty: Dataset[T] = left.sparkSession.emptyDataset[T]

    def emptyDF: DataFrame = left.sparkSession.emptyDataFrame

    def lfilter(col: Column, hkeys: Broadcast[Set[Int]]): Dataset[T] = {
      left.filter(!col.isInCollection(hkeys.value))
    }

    def hfilter(col: Column, hkeys: Broadcast[Set[Int]]): Dataset[T] = {
      left.filter(col.isInCollection(hkeys.value))
    }

    def equiJoin[S: Encoder : ClassTag](right: Dataset[S], usingColumns: Seq[String]): DataFrame = {
      left.join(right, col(usingColumns(0)) === col(usingColumns(1)))
    }

  }

  implicit class SkewDataframeKeyOps(dfs: (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]])) extends Serializable {

    val light = dfs._1
    val heavy = dfs._2
    val key = dfs._3
    val heavyKeys = dfs._4

    def print: Unit = (light, heavy).print

    def select(col: String, cols: String*): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]])= {
      (light.select(col, cols:_*), heavy.select(col, cols:_*), key, heavyKeys)
    }

    def as[U: Encoder : TypeTag]: (Dataset[U], Dataset[U], Option[String], Broadcast[Set[Int]]) = {
      if (heavy.rdd.getNumPartitions == 0) (light.as[U], light.sparkSession.emptyDataset[U], key, heavyKeys)
      else (light.as[U], heavy.as[U], key, heavyKeys)
    }

    def withColumn(colName: String, col: Column): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      (light.withColumn(colName, col), heavy.withColumn(colName, col), key, heavyKeys)
    }

    def withColumnRenamed(existingName: String, newName: String): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      (light.withColumnRenamed(existingName, newName), 
        heavy.withColumnRenamed(existingName, newName), key, heavyKeys)
    }

  }

  implicit class SkewDatasetKeyOps[T: Encoder : ClassTag](dfs: (Dataset[T], Dataset[T], Option[String], Broadcast[Set[Int]])) extends Serializable {
    val light = dfs._1
    val heavy = dfs._2
    val key = dfs._3 
    val heavyKeys = dfs._4
    val partitions = light.rdd.getNumPartitions

    def print: Unit = (light, heavy).print

    def count: Long = (light, heavy).count

    def cache: Unit = (light, heavy).cache

    // don't repartition a set with known heavy keys
    def repartition(partitionExprs: Column*): (Dataset[T], Dataset[T], Option[String], Broadcast[Set[Int]]) = {
      println("can't repartition when heavy keys are known")
      dfs
    }

    def union: Dataset[T] = (light, heavy).union

    def select(col: String, cols: String*): (DataFrame, DataFrame) = {
      (light.select(col, cols:_*), heavy.select(col, cols:_*))
    }

    def as[U: Encoder : TypeTag]: (Dataset[U], Dataset[U], Option[String], Broadcast[Set[Int]]) = {
      (light.as[U], heavy.as[U], key, heavyKeys)
    }

    def withColumn(colName: String, col: Column): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      (light.withColumn(colName, col), heavy.withColumn(colName, col), key, heavyKeys)
    }

    def withColumnRenamed(existingName: String, newName: String): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      key match{
        case Some(k) if k == existingName =>
        (light.withColumnRenamed(existingName, newName), 
          heavy.withColumnRenamed(existingName, newName), Some(newName), heavyKeys)
        case _ => 
          (light.withColumnRenamed(existingName, newName), 
            heavy.withColumnRenamed(existingName, newName), key, heavyKeys)
      }

    }

    def equiJoin[S: Encoder : ClassTag](right: (Dataset[S], Dataset[S]), usingColumns: Seq[String]): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      // using a heavy key
      val hkeys = key match {
        case Some(k) if usingColumns.contains(k) => heavyKeys
        case _ => light.sparkSession.sparkContext.broadcast(Set.empty[Int])
      }
      if (hkeys.value.nonEmpty){
        val rkey = usingColumns(1)
        val runion = right.union
        val lresult = light.join(runion.lfilter(col(rkey), hkeys), col(key.get) === col(rkey))

        val hresult = heavy.join(runion.hfilter(col(rkey), hkeys).hint("broadcast"), col(key.get) === col(rkey))
        (lresult, hresult, key, hkeys)
      // using a key we know is not heavy
      }else{
        (light, heavy).equiJoin(right, usingColumns)
      }

    }

  }

  implicit class SkewDataframeOps(dfs: (DataFrame, DataFrame)) extends Serializable {

    val light = dfs._1
    val heavy = dfs._2

    def print: Unit = {
      println("light")
      light.collect.foreach(println(_))
      println("heavy")
      heavy.collect.foreach(println(_))
    }

    def select(col: String, cols: String*): (DataFrame, DataFrame) = {
      (light.select(col, cols:_*), heavy.select(col, cols:_*))
    }

    def as[U: Encoder : TypeTag]: (Dataset[U], Dataset[U]) = {
      (light.as[U], heavy.as[U])
    }

    def withColumn(colName: String, col: Column): (DataFrame, DataFrame) = {
      (light.withColumn(colName, col), heavy.withColumn(colName, col))
    }

    def withColumnRenamed(existingName: String, newName: String): (DataFrame, DataFrame) = {
      (light.withColumnRenamed(existingName, newName), 
        heavy.withColumnRenamed(existingName, newName))
    }

  }

  implicit class SkewDatasetOps[T: Encoder : ClassTag](dfs: (Dataset[T], Dataset[T])) extends Serializable {
    
    val light = dfs._1
    val heavy = dfs._2
    val partitions = light.rdd.getNumPartitions
    val random = scala.util.Random

    def print: Unit = {
      println("light")
      light.collect.foreach(println(_))
      println("heavy")
      heavy.collect.foreach(println(_))
    }

    def count: Long = {
      val lc = light.count
      val hc = heavy.count
      lc + hc
    }

    def cache: Unit = {
      light.cache
      heavy.cache
    }

    def repartition(partitionExprs: Column*): (Dataset[T], Dataset[T]) = {
      (light.repartition(partitionExprs:_*), heavy.repartition(partitionExprs:_*))
    }

    def union: Dataset[T] = if (heavy.rdd.getNumPartitions == 0) light 
      else light.union(heavy)

    def select(col: String, cols: String*): (DataFrame, DataFrame) = {
      (light.select(col, cols:_*), heavy.select(col, cols:_*))
    }

    def as[U: Encoder : TypeTag]: (Dataset[U], Dataset[U]) = {
      (light.as[U], heavy.as[U])
    }

    def withColumn(colName: String, col: Column): (DataFrame, DataFrame) = {
      (light.withColumn(colName, col), heavy.withColumn(colName, col))
    }

    def withColumnRenamed(existingName: String, newName: String): (DataFrame, DataFrame) = {
      (light.withColumnRenamed(existingName, newName), 
        heavy.withColumnRenamed(existingName, newName))
    }

    def heavyKeys(key: String): (Dataset[T], Set[Int]) = {
      val dfull = dfs.union
      val keys = dfull.select(key).rdd.mapPartitions(it => {
        var cnt = 0
        val acc = HashMap.empty[Row, Int].withDefaultValue(0)
        it.foreach{ c => cnt +=1; if (random.nextDouble <= .1) acc(c) += 1 }
        acc.filter(_._2 > (cnt*.1)*.0025).map(r => r._1.getInt(0)).iterator
      }).collect.toSet
      (dfull, keys)
    }

    def equiJoin[S: Encoder : ClassTag](right: (Dataset[S], Dataset[S]), usingColumns: Seq[String]): (DataFrame, DataFrame, Option[String], Broadcast[Set[Int]]) = {
      val nkey = usingColumns(0)
      val (dfull, hk) = heavyKeys(nkey)
      if (hk.nonEmpty){
        val hkeys = dfull.sparkSession.sparkContext.broadcast(hk)
        val rkey = usingColumns(1)
        (dfull.lfilter(col(nkey), hkeys), dfull.hfilter(col(nkey), hkeys), Some(nkey), hkeys).equiJoin(right, usingColumns)
      }else{
        (dfull.equiJoin(right.union, usingColumns), light.emptyDF, Some(nkey), light.sparkSession.sparkContext.broadcast(Set.empty[Int]))
      }
    }


  }

}