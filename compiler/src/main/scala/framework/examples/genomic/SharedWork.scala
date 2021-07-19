package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

trait SharedQuery extends DriverGene {

  // val clinDir = "/nfs_qc4/genomics/gdc/biospecimen/clinical/patient/"
  val clinDir = "/Users/jac/data/dlbc/nationwidechildrens.org_clinical_patient_dlbc.txt"
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val clinical = spark.table("clinical")
          |val IBag_clinical__D = clinical
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
          |val clinical = spark.table("clinical")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
              	  "clinical" -> BagType(tcgaType))

}

object SW0 extends SharedQuery {

	val name = "SW0"

  val parser = Parser(tbls)

	val query = (n: String) =>
		s"""
			TumorSites${n} <= dedup(for c in clinical union 
				{(tsite := c.tumor_tissue_site)});

			$n <=
				for t in TumorSites${n} union 
					{(tsite := t.tsite, sids :=
						for c in clinical union
							if (t.tsite = c.tumor_tissue_site) then
							for s in samples union 
								if (s.bcr_patient_uuid = c.sample)
								then {( sid := s.bcr_patient_uuid, 
									    aid := s.bcr_aliquot_uuid, 
									    hist := c.histological_type, 
									    tissue := c.tumor_tissue_site )} )}
		"""

    val program = parser.parse(query(name)).get.asInstanceOf[Program]

}

object SW0a extends SharedQuery {
	val name = "SW0a"
	val parser = Parser(tbls)
	val query = SW0.query(name)
	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW1 extends SharedQuery {

	val name = "SW1"

  val parser = Parser(tbls)

	val query = (n: String) =>
		s"""
			$n <=
				for s in samples union 
					for c in clinical union 
						if (s.bcr_patient_uuid = c.sample)
						then {( sid := s.bcr_patient_uuid, 
							    aid := s.bcr_aliquot_uuid, 
							    hist := c.histological_type, 
							    tissue := c.tumor_tissue_site )}
		"""

    val program = parser.parse(query(name)).get.asInstanceOf[Program]

}

object SW1a extends SharedQuery {
	val name = "SW1a"
	val parser = Parser(tbls)
	val query = SW1.query(name)
	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW2 extends SharedQuery {

	val name = "SW2"

  	val parser = Parser(tbls)

	val query = 
		s"""
			SW2 <=
		        for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
		            then {(aid := s.bcr_aliquot_uuid, sid := s.bcr_patient_uuid, 
		            	gene := c.cn_gene_id, cnum := c.cn_copy_number)}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

// same as SW2 just with a filter
object FW1 extends SharedQuery {

	val name = "FW1"

  	val parser = Parser(tbls)

	val query = 
		s"""
			FW1 <=
		        for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid && c.min_copy_number > 0)
		            then {(aid := s.bcr_aliquot_uuid, sid := s.bcr_patient_uuid, 
		            	gene := c.cn_gene_id, cnum := c.cn_copy_number)}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

// same as above two but with a different filter
object FW2 extends SharedQuery {

	val name = "FW2"

  	val parser = Parser(tbls)

	val query = 
		s"""
			FW2 <=
		        for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid && c.max_copy_number > 0)
		            then {(aid := s.bcr_aliquot_uuid, sid := s.bcr_patient_uuid, 
		            	gene := c.cn_gene_id, cnum := c.cn_copy_number)}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW3 extends SharedQuery {

	val name = "SW3"

  	val parser = Parser(tbls)

	val query = 
		s"""
			SW3 <=
		        for s in samples union 
		    		{(sid := s.bcr_patient_uuid, aid := s.bcr_aliquot_uuid, cnvs := 
			          for c in copynumber union 
			            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
			            then {( gene := c.cn_gene_id, cnum := c.cn_copy_number, 
			            	cmax := c.max_copy_number, cmin := c.min_copy_number )} )}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW4 extends SharedQuery {

	val name = "SW4"

  	val parser = Parser(tbls)

  	val cnvAvg = "(((c.cn_copy_number + c.max_copy_number) + c.min_copy_number) + 0.01) / 3.0"

	val query = 
		s"""
			SW4 <=
		        for s in samples union 
		    		{(sid := s.bcr_patient_uuid, aid := s.bcr_aliquot_uuid, cnvs := 
			          for c in copynumber union 
			            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
			            then {( gene := c.cn_gene_id, cavg := $cnvAvg )} )}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW5 extends SharedQuery {

	val name = "SW5"

  	val parser = Parser(tbls)

	val query = 
		s"""
			SW5 <=
		        (for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
		            then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, 
		            	cnum := c.cn_copy_number + 0.01 )}).sumBy({sid, gene}, {cnum})
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW6 extends SharedQuery {

	val name = "SW6"

  	val parser = Parser(tbls)

	val query = 
		s"""
			SW6 <=
		        (for s in samples union 
		          for c in copynumber union 
		            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
		            then {(sid := s.bcr_patient_uuid, cnum := c.cn_copy_number + 0.01 )}).sumBy({sid}, {cnum})
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}

object SW7 extends SharedQuery {

	val name = "SW7"

  	val parser = Parser(tbls)

  	val cnvAvg = "(((c.cn_copy_number + c.max_copy_number) + c.min_copy_number) + 0.01) / 3.0"

	val query = 
		s"""
			SW7 <=
		        for s in samples union 
		    		{(sid := s.bcr_patient_uuid, cnvs := 
			          (for c in copynumber union 
			            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
			            then {( cavg := $cnvAvg )}).sumBy({}, {cavg}) )}
		"""

	val program = parser.parse(query).get.asInstanceOf[Program]
}
