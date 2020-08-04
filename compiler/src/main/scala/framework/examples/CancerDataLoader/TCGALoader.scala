package framework.examples.CancerDataLoader

import java.io.File
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.mutable

case class tcg(sample: String, gender: String, race: String, ethnicity: String, tumor_tissue_site: String, histological_type: String)

class TCGALoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val delimiter: String = "\t"

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }


  def load(path: String, dir: Boolean = false): Dataset[tcg] = {
    val files = if (dir) {
      val files = new File(path)

      val okFileExtensions = List("txt")
      val d = getListOfFiles(files, okFileExtensions).toSeq.map(f => path + "/" + f.getName)
      //      d.foreach(println)
      d
    } else {
      Seq(path)
    }

    var df = Seq.empty[tcg].toDS()

    for (t <- files) {

      // drop the header before going through the file line by line
      val file = spark.sparkContext.textFile(t)

      val rows: Array[String] = file.take(2)
      val header = getHeader(rows)

      if (header.contains("bcr_patient_uuid") && header.contains("histological_type")) {
        val f = file.mapPartitionsWithIndex { (id_x, iter) =>
          if (id_x == 0) {
            iter.drop(3)
          } else iter
        }
        //
        //    print(header.size)

        val data: Dataset[tcg] = f.map(
          line => {
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
      } else {
        //        print(file.name)
      }
    }

    df
  }


  private def getHeader(lines: Array[String]): mutable.HashMap[String, Int] = {
    val dict = mutable.HashMap.empty[String, Int]
    for (line <- lines) {
      val first = line.split(delimiter)
      for (a <- first.indices) {
        val field = first.apply(a)
        dict.put(field, a)
        //        println(field + ", " + a)
      }
    }
    dict
  }
}

