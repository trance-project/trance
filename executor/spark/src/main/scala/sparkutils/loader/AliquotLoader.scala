package framework.examples.CancerDataLoader

import org.apache.parquet.format.IntType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

case class aliquot(bcr_patient_uuid: String,
                   bcr_sample_barcode: String,
                   bcr_aliquot_barcode: String,
                   bcr_aliquot_uuid: String,
                   biospecimen_barcode_bottom: String,
                   center_id: String,
                   concentration: String,
                   date_of_shipment: String,
                   is_derived_from_ffpe: String,
                   plate_column: String,
                   plate_id: String,
                   plate_row: String,
                   quantity: String,
                   source_center: String,
                   volume: String)

class AliquotLoader(spark: SparkSession) {

  import spark.implicits._

  val delimiter: String = "\t"

  def load(path: String): Dataset[aliquot] = {

    val data: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("comment", "#")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .option("comment", "\t")
      .load(path)
    data.toDF().as[aliquot]
  }
}


