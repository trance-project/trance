package framework.examples.CancerDataLoader

import java.io.File
import scala.io.Source

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.Row.fromSeq

import scala.collection.mutable
import scala.collection.mutable.HashMap

case class tcg(sample: String, gender: String, race:String, ethnicity:String, tumor_tissue_site: String, histological_type: String)

class TCGALoader(spark: SparkSession) {

  import spark.implicits._

  val delimiter: String = "\t"

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }


  def load(path: String, dir: Boolean = false): Dataset[tcg] = {
    val files = if (dir){
      val files = new File(path)

      val okFileExtensions = List("txt")
      getListOfFiles(files, okFileExtensions).toSeq.map(f => f.getName)
    }else Seq(path)

    var df = Seq.empty[tcg].toDS()

    for(file <- files){

      val header = getHeader(read(file))
      // drop the header before going through the file line by line
      val f = spark.sparkContext.textFile(file)
        .mapPartitionsWithIndex { (id_x, iter) => if (id_x < 3) iter.drop(1) else iter }

//      print(header.size)

      val data: Dataset[tcg] = f.map(
        line =>{
          val sline = line.split(delimiter)
          val sample = sline(header("bcr_patient_uuid"))
          val gender = sline(header("gender"))
          val race = sline(header("race"))
          val ethnicity = sline(header("ethnicity"))
          val tumor_tissue_site = sline(header("tumor_tissue_site"))
          val histological_type = sline(header("histological_type"))

          tcg(sample, gender, race, ethnicity, tumor_tissue_site, histological_type)
        }
      ).toDF().as[tcg]
      df = df.union(data)
    }
    df
  }

  private def read(path: String) = {

    val lines = Source.fromFile(path).getLines
    val first = lines.next()
    val second = lines.next()

    (first.split(delimiter), second.split(delimiter))

  }

  private def getHeader(path: String): mutable.HashMap[String, Int] = {


    val (first, second) = read(path)
    val dict = mutable.HashMap.empty[String, Int]

    for(i <- df.indices){

      val row = df(i)
      val header: Seq[StructField] = row.schema.toList
      for(a <- header.indices){
        val field = header.apply(a)
        dict.put(field.name, a)

        println(field.name + ", "+a)
      }
    }
    dict
  }
}

