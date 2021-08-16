package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

object FlatToNestedBio extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
      if (shred){
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val IBag_samples__D = loader.loadSamples()
	        |val IBag_copynumber__D = loader.loadCopyNum()        
	        |val (odict1, odict2, odict3) = loader.loadOccurrShred()
	        |val IBag_occurrences__D = odict1
	        |val IMap_occurrences__D_transcript_consequences = odict2
	        |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
	      """.stripMargin
	    }else{
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val samples = loader.loadSamples()
	        |val copynumber = loader.loadCopyNum()   
	        |val occurrences = loader.loadOccurrences()
	      """.stripMargin
	    }

  val name = "FlatToNestedBio"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

  // all samples that have a TP53 mutation with non-high impact
  val query = 
    s"""
      FlatToNestedBio <= 
        for s in samples union 
          {( sid := s.bcr_patient_uuid, cnvs := 
          	for c in copynumber union 
              if ( s.bcr_aliquot_uuid == c.cn_aliquot_uuid )
              then {( gene := c.cn_gene_id, cnum := c.cn_copy_number + 0.01 )}
          )}
    """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object NestedToNestedBio extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
      if (shred){
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val IBag_samples__D = loader.loadSamples()
	        |val IBag_copynumber__D = loader.loadCopyNum()        
	        |val (odict1, odict2, odict3) = loader.loadOccurrShred()
	        |val IBag_occurrences__D = odict1
	        |val IMap_occurrences__D_transcript_consequences = odict2
	        |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
	      """.stripMargin
	    }else{
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val samples = loader.loadSamples()
	        |val copynumber = loader.loadCopyNum()   
	        |val occurrences = loader.loadOccurrences()
	      """.stripMargin
	    }

  val name = "NestedToNestedBio"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

  val imp = """if (t.impact = "HIGH") then 0.80 
              else if (t.impact = "MODERATE") then 0.50
              else if (t.impact = "LOW") then 0.30
              else 0.01"""
  // all samples that have a TP53 mutation with non-high impact
  val query = 
    s"""
      NestedToNestedBio <= 
        for s in samples union 
          {( sid := s.bcr_patient_uuid, mutations := 
          	for o in occurrences union 
          	  if ( s.bcr_patient_uuid == o.donorId )
          	  then {( contig := o.chromosome,
          	  	 start := o.vstart,
          	  	 end := o.vend,
          	  	 scores := (for t in o.transcript_consequences union 
          	  	 			for c in copynumber union 
          	  	 			  if ( s.bcr_aliquot_uuid == c.cn_aliquot_uuid && c.cn_gene_id == t.gene_id )
          	  	 			  then {( gene := t.gene_id, score := $imp )}).sumBy({gene}, {score})
          	  )}
          )}
    """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object NestedToFlatBio extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
      if (shred){
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val IBag_samples__D = loader.loadSamples()
	        |val IBag_copynumber__D = loader.loadCopyNum()        
	        |val (odict1, odict2, odict3) = loader.loadOccurrShred()
	        |val IBag_occurrences__D = odict1
	        |val IMap_occurrences__D_transcript_consequences = odict2
	        |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
	      """.stripMargin
	    }else{
	      s"""
	        |val loader = new DemoLoader(spark)
	        |val samples = loader.loadSamples()
	        |val copynumber = loader.loadCopyNum()   
	        |val occurrences = loader.loadOccurrences()
	      """.stripMargin
	    }

  val name = "NestedToFlatBio"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

  val imp = """if (t.impact = "HIGH") then 0.80 
              else if (t.impact = "MODERATE") then 0.50
              else if (t.impact = "LOW") then 0.30
              else 0.01"""
  // all samples that have a TP53 mutation with non-high impact
  val query = 
    s"""
      NestedToFlatBio <= 
      	(for s in samples union 
          for o in occurrences union 
            if ( s.bcr_patient_uuid == o.donorId )
            then for t in o.transcript_consequences union 
          	  {( sid := s.bcr_patient_uuid, gene := t.gene_id, burden := 1.0 )}).sumBy({gene}, {score})
    """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}