package framework.generator.spark

import framework.common._

import framework.plans._
import framework.utils.Utils.ind

class SparkLoaderGenerator(inputs: Map[Type, String] = Map()) extends SparkTypeHandler {

  var types: Map[Type, String] = inputs

  def generateLoader(tp: Type, header: Boolean = true, delimiter: String = ","): String = {
    val tname = types(tp)
    handleType(tp, Some(tname))
    val cname = generateType(tp)
    s"""|package sparkutils.loader
        |/** Generated Code **/
        |import org.apache.spark.sql.Dataset
        |import org.apache.spark.sql.SparkSession
        |import org.apache.spark.sql.types._
        |
        |${generateTypeDef(tp)}
        |
        |class ${tname}Loader(spark: SparkSession) extends Table[$cname] {
        | 
        |   import spark.implicits._
        |   val schema = StructType(${generateSqlType(tp)})
        |   val header: Boolean = $header
        |   val delimiter: String = "$delimiter"
        |   
        |   def load(path: String): Dataset[$cname] = {
        |     spark.read.schema(schema)
        |       .option("header", header)
        |       .option("delimiter", delimiter)
        |       .csv(path)
        |       .as[$cname]        
        |   }
        |}
        |""".stripMargin
  }

}