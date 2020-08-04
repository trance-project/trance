package sparkutils.loader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import scala.io.Source
import java.io.File

case class Biospec(bcr_patient_uuid: String, bcr_sample_barcode: String, bcr_aliquot_barcode: String, bcr_aliquot_uuid: String, biospecimen_barcode_bottom: String, center_id: String, concentration: Double, date_of_shipment: String, is_derived_from_ffpe: String, plate_column: Int, plate_id: String, plate_row: String, quantity: Double, source_center: Int, volume: Double)

class BiospecLoader(spark: SparkSession) extends Serializable{

   import spark.implicits._
   val schema = StructType(Array(StructField("bcr_patient_uuid", StringType),
      StructField("bcr_sample_barcode", StringType),
      StructField("bcr_aliquot_barcode", StringType),
      StructField("bcr_aliquot_uuid", StringType),
      StructField("biospecimen_barcode_bottom", StringType),
      StructField("center_id", StringType),
      StructField("concentration", DoubleType),
      StructField("date_of_shipment", StringType),
      StructField("is_derived_from_ffpe", StringType),
      StructField("plate_column", IntegerType),
      StructField("plate_id", StringType),
      StructField("plate_row", StringType),
      StructField("quantity", DoubleType),
      StructField("source_center", IntegerType),
      StructField("volume", DoubleType)))

   val header: Boolean = true
   val delimiter: String = "\t"

   def load(path: String, dir: Boolean = false): Dataset[Biospec] = {

      val files = if (dir){
         val files = new File(path)
         files.listFiles().toSeq.map(f => f.getName)
      }else Seq(path)

      spark.read.schema(schema)
        .format("csv")
        .option("header", "true")
        .option("comment", "#")
        .option("delimiter", delimiter)
        .option("inferSchema", "true")
        .load(files:_*)
        .na.drop.as[Biospec].repartition(400)

   }
}

