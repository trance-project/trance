package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

// this is just the example from the 
object ExampleQuery extends DriverGene {
  
  // this will be reviewed next
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String = 
    if (shred){
      s""// TODO""
    }else{
      s"""|val sloader = new BiospecLoader(spark)
          |val samples = sloader.load("/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_dlbc.txt")
          |val cloader = new CopyNumberLoader(spark)
          |val copynumber = cloader.load("/mnt/app_hdd/data/cnv", true)
          |
          |val geLoader = new GeneExpressionLoader(spark)
          |val expression = geLoader.load("/mnt/app_hdd/data/expression", true)
          |
          |val occurrences = spark.read.json("/mnt/app_hdd/data/somatic/datasetPRAD")
          |""".stripMargin
    }
  
  // name to identify your query
  val name = "ExampleQuery"
  
  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "samples" -> samples.tp)

  // a query string that is passed to the parser
  // note that a list of assignments should be separated with ";"
  val query = 
    s"""
      cnvCases1 <= 
        for s in samples union 
          for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)}


    """

    // finally define the parser, note that it takes the input types 
    // map as input and pass the query string to the parser to 
    // generate the program.
    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}