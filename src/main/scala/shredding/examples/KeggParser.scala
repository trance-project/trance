package shredding.examples

import org.apache.spark.{SparkConf, SparkContext}

object KeggParser {

  /** defines a genomic path with name and description url.
    * the file /home/gabor/Downloads/c2.cp.v6.2.symbols.gmt is downloaded from
    *     http://software.broadinstitute.org/gsea/msigdb/download_file.jsp?filePath=/resources/msigdb/6.2/c2.cp.v6.2.symbols.gmt
    * @param sName          the name. For example: KEGG_GLYCOLYSIS_GLUCONEOGENESIS
    * @param descriptionUrl example: http://www.broadinstitute.org/gsea/msigdb/cards/KEGG_GLYCOLYSIS_GLUCONEOGENESIS
    * @param path           example: ACSS2	GCK	PGK2	PGK1	PDHB	PDHA1	PDHA2	PGM2	TPI1	ACSS1	FBP1	ADH1B	HK2	ADH1C	HK1	HK3	ADH4	PGAM2	ADH5	PGAM1	ADH1A [...]
    */
  case class Pathway(sName: String, descriptionUrl: String, path: Array[String]);

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KeggParser").setMaster("local")
    val sc = new SparkContext("local", "KeggParser", conf)
    val inputFile = sc.textFile("/home/gabor/Downloads/c2.cp.v6.2.symbols.gmt", 3)
    //val results = input.mapPartitions(_=>createProcess(_.mkString),true)
    val pathways = inputFile.map(line => {
      val fields = line.split("\t| ")
      Pathway(fields(0), fields(1), fields.tail.tail)
    }).collect()

    pathways.foreach(println(_))
    println("first pathway:" + pathways.head.path.mkString(","))
    println("size:" + pathways.size)
    sc.stop()
  }

}
