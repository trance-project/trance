package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import sparkutils.Config

/** Biospecimen loader designed from the GDC/ICGC biospecimen aliquot 
  * files (bcr biotab). Such as: 
  * https://portal.gdc.cancer.gov/files/e5ebf196-d464-4592-895e-addd43851c16
  *
  **/

case class Biospec(bcr_patient_uuid: String, bcr_sample_barcode: String, bcr_aliquot_barcode: String, bcr_aliquot_uuid: String, biospecimen_barcode_bottom: String, center_id: String, concentration: Double, date_of_shipment: String, is_derived_from_ffpe: String, plate_column: Int, plate_id: String, plate_row: String, quantity: Double, source_center: Int, volume: Double)

class BiospecLoader(spark: SparkSession) extends Table[Biospec] {
 
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
   
   def load(path: String): Dataset[Biospec] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path).na.drop.as[Biospec].repartition(Config.minPartitions)
   }

   def store(path: String, insert: Boolean = true, name: String = "samples") = {

    val samples = load(path)
    val stype = List(
      "bcr_patient_uuid string", "bcr_sample_barcode string", 
      "bcr_aliquot_barcode string", "bcr_aliquot_uuid string",
      "biospecimen_barcode_bottom string", "center_id string", 
      "concentration double", "date_of_shipment string", 
      "is_derived_from_ffpe string", "plate_column int",
      "plate_id string", "plate_row string",
      "quantity double", "source_center int", "volume double")

    spark.sql(s"""CREATE TABLE IF NOT EXISTS $name(${stype.mkString(", ")}) USING hive""")

    // insert into hive
   if (insert){ 
	samples.write.insertInto(name)

    	// validate
    	spark.sql(s"SELECT * FROM $name").show
    }
  }

}

