package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

object LetTest0 extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IDict_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IDict_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest0"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val query = 
    	s"""
	      	let cnvCases := 
		        for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
		            then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)}
		    in 
    			(for o in occurrences union 
    				for t in o.transcript_consequences union 
    					for c in cnvCases union 
    						if (o.donorId = c.sid && t.gene_id = c.gene)
    						then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := $imp * (c.cnum + 0.01))}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score})
    	"""
    	    		// let initScores :=
    	// 	in
    	// 	for s in samples union 
    	// 		{( hybrid_sample := s.bcr_patient_uuid,
    	// 		   hybrid_aliquot := s.bcr_aliquot_uuid,
    	// 		   hybrid_center := s.center_id,
    	// 		   hybrid_genes := for i in initScores union
    	// 		   	if (s.bcr_patient_uuid = i.hybrid_case)
    	// 		   	then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_gene)})}
    	// """

    val parser = Parser(tbls)
    // val program = parser.parse(query).get.asInstanceOf[Program]
    val bagexpr = parser.parse(query, parser.term).get.asInstanceOf[BagExpr]
    val program = Program(Assignment(name, bagexpr))

}