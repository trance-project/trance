package sparkutils.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import sparkutils.Config

/** Reads the clinical table from the CONVERGE dataset.
  * https://www.med.unc.edu/pgc/download-results/converge/
  *
  **/ 

case class Converge(gad: Int, city: String, fh_count: Int, dep_e29: Int, marital_status: Int, md_a4: Int, height_clean: Int, animal_diag: Int, bmi: Double, md_a8: Int, prot_m: Int, weight_clean: Int, md_a3: Int, postnatal_d: Int, suicidal_attempt: Int, situational_diag: Int, md_a5: Int, cold_f: Int, agora_diag: Int, md_a9: Int, csa: Int, age: Int, dob_d: Int, blood_diag: Int, prot_f: Int, id: String, neuroticism: Int, education: Int, me: Int, md_a6: Int, sle: Int, dep_e27: Int, dysthymia: Int, social_diag: Int, province: String, social_class: Int, md_a1: Int, hospital_code: String, cold_m: Int, occupation: Int, srr_id: String, iscase: Int, suicidal_thought: Int, auth_f: Int, dob_m: Int, suicidal_plan: Int, panic: Int, md_a7: Int, pms: Int, md_a2: Int, dob_y: Int, phobia: Int, index: Int, auth_m: Int)

class ConvergeLoader(spark: SparkSession) extends Table[Converge] {
 
   import spark.implicits._
   val schema = StructType(Array(StructField("gad", IntegerType),
    StructField("city", StringType),
    StructField("fh_count", IntegerType),
    StructField("dep_e29", IntegerType),
    StructField("marital_status", IntegerType),
    StructField("md_a4", IntegerType),
    StructField("height_clean", IntegerType),
    StructField("animal_diag", IntegerType),
    StructField("bmi", DoubleType),
    StructField("md_a8", IntegerType),
    StructField("prot_m", IntegerType),
    StructField("weight_clean", IntegerType),
    StructField("md_a3", IntegerType),
    StructField("postnatal_d", IntegerType),
    StructField("suicidal_attempt", IntegerType),
    StructField("situational_diag", IntegerType),
    StructField("md_a5", IntegerType),
    StructField("cold_f", IntegerType),
    StructField("agora_diag", IntegerType),
    StructField("md_a9", IntegerType),
    StructField("csa", IntegerType),
    StructField("age", IntegerType),
    StructField("dob_d", IntegerType),
    StructField("blood_diag", IntegerType),
    StructField("prot_f", IntegerType),
    StructField("id", StringType),
    StructField("neuroticism", IntegerType),
    StructField("education", IntegerType),
    StructField("me", IntegerType),
    StructField("md_a6", IntegerType),
    StructField("sle", IntegerType),
    StructField("dep_e27", IntegerType),
    StructField("dysthymia", IntegerType),
    StructField("social_diag", IntegerType),
    StructField("province", StringType),
    StructField("social_class", IntegerType),
    StructField("md_a1", IntegerType),
    StructField("hospital_code", StringType),
    StructField("cold_m", IntegerType),
    StructField("occupation", IntegerType),
    StructField("srr_id", StringType),
    StructField("iscase", IntegerType),
    StructField("suicidal_thought", IntegerType),
    StructField("auth_f", IntegerType),
    StructField("dob_m", IntegerType),
    StructField("suicidal_plan", IntegerType),
    StructField("panic", IntegerType),
    StructField("md_a7", IntegerType),
    StructField("pms", IntegerType),
    StructField("md_a2", IntegerType),
    StructField("dob_y", IntegerType),
    StructField("phobia", IntegerType),
    StructField("index", IntegerType),
    StructField("auth_m", IntegerType)))
   val header: Boolean = true
   val delimiter: String = ","
   
   def load(path: String): Dataset[Converge] = {
     spark.read.schema(schema)
       .option("header", header)
       .option("delimiter", delimiter)
       .csv(path)
       .as[Converge].repartition(Config.minPartitions) 
   }
}

