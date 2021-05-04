package framework.examples.genomic

import java.util.Base64
import framework.common._
import framework.examples.Query
import framework.nrc.Parser
// test2

// this is just the example from the 
object ExampleQuery extends DriverGene {
  
  // TODO: update this with the loading functionality in ExampleQuery2
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
          |
          |val occurrences = spark.read.json("/mnt/app_hdd/data/somatic/datasetPRAD")
          |val ploader = new PathwayLoader(spark)
          |val pathways = ploader.load("/mnt/app_hdd/data/pathway/c2.cp.v7.1.symbols.gmt")
          |val gtfLoader = new GTFLoader(spark, "/nfs_qc4/genomics/Homo_sapiens.GRCh37.87.chr.gtf")
          |val genemap = gtfLoader.loadDS
          |""".stripMargin
    }
  
  // name to identify your query
  val name = "ExampleQuery"
  // moved these to DriverGenes.scala Pathway train (line 19)
  // val genetype = TupleType("name" -> StringType)
  // val pathtype = TupleType("p_name" -> StringType, "url" -> StringType, "gene_set" -> BagType(genetype))

  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "samples" -> samples.tp,
                    "pathways" -> pathway.tp,
                    "genemap" -> gtf.tp)


  // a query string that is passed to the parser
  // note that a list of assignments should be separated with ";"
  val query = 
    s"""
        GMB <=
          for g in genemap union
            {(gene:= g.g_gene_name, burdens :=
              (for o in occurrences union
                for t in o.transcript_consequences union
                  if (g.g_gene_id = t.gene_id) then
                     {(sid := o.donorId, burden := if (t.impact = "HIGH") then 0.80
                                                else if (t.impact = "MODERATE") then 0.50
                                                else if (t.impact = "LOW") then 0.30
                                                else 0.01)}).sumBy({sid}, {burden}))}

    """

    // finally define the parser, note that it takes the input types
    // map as input and pass the query string to the parser to
    // generate the program.
    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

// this is just the example from the 
object ExampleQuery2 extends DriverGene {

  val sampleFile = "/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_prad.txt"
  val cnvFile = "/mnt/app_hdd/data/cnv"
  val occurFile = "/mnt/app_hdd/data/somatic/"
  val occurName = "datasetPRAD"
  val occurDicts = ("odictPrad1", "odictPrad2", "odictPrad3")
  val pathFile = "/mnt/app_hdd/data/c2.cp.v7.1.symbols.gmt"
  val gtfFile = "/nfs_qc4/genomics/Homo_sapiens.GRCh37.87.chr.gtf"

  // in DriverGenes.scala you can see traits for several datatypes, these 
  // are inherited from DriverGene trait (around line 549)
  // checkout individuals traits to see what the load functions are doing
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String = 
    s"""|${loadBiospec(shred, skew, fname = sampleFile, name = "samples")}
        |${loadCopyNumber(shred, skew, fname = cnvFile)}
        |${loadOccurrence(shred, skew, fname = occurFile, iname = occurName, dictNames = occurDicts)}
        |${loadPathway(shred, skew, fname = pathFile)}
        |${loadGtfTable(shred, skew, fname = gtfFile)}
        |""".stripMargin
  
  // name to identify your query
  val name = "ExampleQuery2"

  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "samples" -> samples.tp,
                    "pathways" -> pathway.tp,
                    "genemap" -> gtf.tp)


  // a query string that is passed to the parser
  // note that a list of assignments should be separated with ";"
  val query = 
    s"""
        impactScores <= 
          (for o in occurrences union
            for t in o.transcript_consequences union
              {(gid := t.gene_id, sid := o.donorId, burden := if (t.impact = "HIGH") then 0.80
                                                else if (t.impact = "MODERATE") then 0.50
                                                else if (t.impact = "LOW") then 0.30
                                                else 0.01)}).sumBy({gid, sid}, {burden});
        PMB <=
          for p in pathways union
            {(pathway := p.p_name, burdens :=
              (for g in p.gene_set union 
                for g2 in genemap union 
                  if (g.name = g2.g_gene_name)
                  then for o impactScores union 
                    if (g2.g_gene_id = o.gene_id) then
                      {(sid := o.sid, burden := o.burden)}).sumBy({sid}, {burden}))}

    """

    // finally define the parser, note that it takes the input types
    // map as input and pass the query string to the parser to
    // generate the program.
    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}
