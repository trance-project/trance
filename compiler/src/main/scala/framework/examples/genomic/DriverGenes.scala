package framework.examples.genomic

import framework.common._
import framework.examples.Query

/** This file contains the base types for 
  * queries / pipelined analysis of the 
  * occurrences endpoint (TCGA)
  *
  * The rest of the document contains the 
  * queries associated to the pipeline 
  * analysis. Additional queries 
  * can be found in OccurReconstruct.scala and
  * CancerAdditional.scala.
  **/

trait Occurrence extends Vep {

  val occurrence_type = TupleType(
      "oid" -> StringType,
      "aliquotId" -> StringType,
      "donorId" -> StringType, 
      "vend" -> LongType,
      "projectId" -> StringType, 
      "vstart" -> LongType,
      "Reference_Allele" -> StringType,
      "Tumor_Seq_Allele1" -> StringType,
      "Tumor_Seq_Allele2" -> StringType,
      "chromosome" -> StringType,
      "allele_string" -> StringType,
      "assembly_name" -> StringType,
      "end" -> LongType,
      "vid" -> StringType,
      "input" -> StringType,
      "most_severe_consequence" -> StringType,
      "seq_region_name" -> StringType, 
      "start" -> LongType,
      "strand" -> LongType,
      "transcript_consequences" -> BagType(transcriptQuant)
    )

    val occurmid_type = TupleType(
      "oid" -> StringType,
      "donorId" -> StringType, 
      "vend" -> LongType,
      "projectId" -> StringType, 
      "vstart" -> LongType,
      "Reference_Allele" -> StringType,
      "Tumor_Seq_Allele1" -> StringType,
      "Tumor_Seq_Allele2" -> StringType,
      "chromosome" -> StringType,
      "allele_string" -> StringType,
      "assembly_name" -> StringType,
      "end" -> LongType,
      "vid" -> StringType,
      "input" -> StringType,
      "most_severe_consequence" -> StringType,
      "seq_region_name" -> StringType, 
      "start" -> LongType,
      "strand" -> LongType,
      "transcript_consequences" -> BagType(transcriptFull)
    )

    def loadOccurrence(shred: Boolean = false, skew: Boolean = false): String = {
    	 val basepath = "/nfs_qc4/genomics/gdc/"
    	val loadFun = if (skew) "shredSkew" else "shred"
    	if (shred){
	    	s"""|val vepLoader = new VepLoader(spark)
    				|//val mafLoader = new MAFLoader(spark)
    				|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
    		    |//val maf = mafLoader.loadFlat(s"$basepath/somatic/mafs/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
    				|//val (occurs, annots) = vepLoader.loadOccurrences(maf)
    				|//val occurrences = vepLoader.buildOccurrences(occurs, annots)
    				|//val (odict1, odict2, odict3) = vepLoader.$loadFun(occurrences)
    				|//val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence]
    				|//val (odict1, odict2, odict3) = vepLoader.$loadFun(vepLoader.finalize(occurrences, biospec))
    				|//val (odict1, odict2, odict3) = vepLoader.$loadFun(vepLoader.finalize(occurrences))
    				|val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
    				|//val occurrences = vepLoader.loadOccurrencesMid(maf)
            |val (odict1, odict2, odict3) = vepLoader.${loadFun}Mid(occurrences)
    		  	|val IBag_occurrences__D = odict1
    				|IBag_occurrences__D.cache
    				|IBag_occurrences__D.count
    				|val IDict_occurrences__D_transcript_consequences = odict2
    				|IDict_occurrences__D_transcript_consequences.cache
    				|IDict_occurrences__D_transcript_consequences.count
    				|val IDict_occurrences__D_transcript_consequences_consequence_terms = odict3
    				|IDict_occurrences__D_transcript_consequences_consequence_terms.cache
    				|IDict_occurrences__D_transcript_consequences_consequence_terms.count
    				|""".stripMargin
    	}else if (skew){
    		s"""|val vepLoader = new VepLoader(spark)
    				|//val mafLoader = new MAFLoader(spark)
    				|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
    	      |//val maf = mafLoader.loadFlat(s"$basepath/somatic/mafs/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
    				|//val (occurs, annots) = vepLoader.loadOccurrences(maf)
    				|//val occurrences_L = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))
    				|//val occurrences_L = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence], biospec_L)
    				|//val occurrences_L = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
    				|val occurrences_L = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
    				|//val occurrences_L = vepLoader.loadOcccurrencesMid(maf)
            |val occurrences = (occurrences_L, occurrences_L.empty)
    				|occurrences.cache
    				|occurrences.count
    				|""".stripMargin
    	}else{
			s"""|val vepLoader = new VepLoader(spark)
				  |//val mafLoader = new MAFLoader(spark)
	      	|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
	      	|//val maf = mafLoader.loadFlat(s"$basepath/somatic/mafs/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
  				|//val (occurs, annots) = vepLoader.loadOccurrences(maf)
  				|//val occurrences = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))
  				|//val occurrences = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence], biospec)
  				|//val occurrences = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
  				|val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
  				|//val occurrences = vepLoader.loadOccurrencesMid(maf)
          |occurrences.cache
  				|occurrences.count
  				|""".stripMargin
    	}
    }

}

trait Gistic {

  def loadGistic(shred: Boolean = false, skew: Boolean = false): String = {
  	if (shred){
  		val loadFun = if (skew) "shredSkew" else "shred"
  		s"""|val gisticLoader = new GisticLoader(spark)
    			|//val gistic = gisticLoader.merge(s"/nfs_qc4/genomics/gdc/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
    			|//                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]				
    			|val gistic = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
    			|				.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
    			|val (gdict1, gdict2) = gisticLoader.$loadFun(gistic)
    			|val IBag_gistic__D = gdict1
    			|IBag_gistic__D.cache
    			|IBag_gistic__D.count
    			|val IDict_gistic__D_gistic_samples = gdict2
    			|IDict_gistic__D_gistic_samples.cache
    			|IDict_gistic__D_gistic_samples.count""".stripMargin
  	}else if (skew)
	  	s"""|val gistic_L = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
    			|					.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
    			|//val gisticLoader = new GisticLoader(spark)
    			|//val gistic_L = gisticLoader.merge(s"/nfs_qc4/genomics/gdc/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
    			|//                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]				
    			|val gistic = (gistic_L, gistic_L.empty)
    			|gistic.cache
    			|gistic.count""".stripMargin
	else
		s"""|val gistic = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
  			|				.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
  			|//val gisticLoader = new GisticLoader(spark)
  			|//val gistic = gisticLoader.merge(s"/nfs_qc4/genomics/gdc/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
  			|//                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]				
  			|gistic.cache
  			|gistic.count""".stripMargin
  }

  val sampleType = TupleType("gistic_sample" -> StringType, 
    "focal_score" -> LongType)

  val gisticType = TupleType("gistic_gene" -> StringType, "gistic_gene_iso" -> StringType, 
  	"cytoband" -> StringType, "gistic_samples" -> BagType(sampleType))

}

trait StringNetwork {

	def loadFlatNetwork(shred: Boolean = false, skew: Boolean = false): String = {
  	if (shred){
  		val fnetLoad = if (skew) "(stringNetwork, stringNetwork.empty)" else "stringNetwork"
  		s"""|val stringLoader = new NetworkLoader(spark)
    			|val stringNetwork = stringLoader.loadFlat("/nfs_qc4/genomics/9606.protein.links.full.v11.0.txt")
    			|val IBag_stringNetwork__D = $fnetLoad
    			|IBag_stringNetwork__D.cache
    			|IBag_stringNetwork__D.count""".stripMargin
  	}else if (skew){
  		s"""|val stringLoader = new NetworkLoader(spark)
    			|val stringNetwork_L = stringLoader.loadFlat("/nfs_qc4/genomics/mart_export.txt")
    			|val stringNetwork = (stringNetwork_L, stringNetwork_L.empty)
    			|stringNetwork.cache
    			|stringNetwork.count""".stripMargin
  	}else{
  		s"""|val stringLoader = new NetworkLoader(spark)
    			|val stringNetwork = stringLoader.loadFlat("/nfs_qc4/genomics/mart_export.txt")
    			|stringNetwork.cache
    			|stringNetwork.count""".stripMargin
  	}
  }

  def loadNetwork(shred: Boolean = false, skew: Boolean = false): String = {
    val nfile = "nfs_qc4/genomics/9606.protein.links.full.v11.0.csv"
	if (shred){
      val fnetLoad = if (skew) "(network, network.empty)" else "network"
      val loadFun = if (skew) "loadSkew" else "loadShred"
      s"""|val stringLoader = new NetworkLoader(spark)
          |val network = stringLoader.$loadFun("$nfile")
          |val IBag_network__D = network._1
          |IBag_network__D.cache
          |IBag_network__D.count
          |val IDict_network__D_edges = network._2
          |IDict_network__D_edges.cache
          |IDict_network__D_edges.count
          |""".stripMargin
    }else if (skew){
      s"""|val stringLoader = new NetworkLoader(spark)
          |val network_L = stringLoader.load("$nfile")
          |val network = (network_L, network_L.empty)
          |network.cache
          |network.count""".stripMargin
    }else{
      s"""|val stringLoader = new NetworkLoader(spark)
          |val network = stringLoader.load("$nfile")
          |network.cache
          |network.count""".stripMargin
    }
  }

  val edgeOrderedType = List(("edge_protein", StringType), ("neighborhood", IntType),
   ("neighborhood_transferred", IntType), ("fusion", IntType), ("cooccurence", IntType),
   ("homology", IntType), ("coexpression", IntType), ("coexpression_transferred", IntType),
   ("experiments", IntType), ("experiments_transferred", IntType),
   ("database", IntType), ("database_transferred", IntType),
  ("textmining", IntType), ("textmining_transferred", IntType), ("combined_score", IntType))

  val edgeType = TupleType("edge_protein" -> StringType, "neighborhood" -> IntType,
   "neighborhood_transferred" -> IntType, "fusion" -> IntType, "cooccurence" -> IntType,
   "homology" -> IntType, "coexpression" -> IntType, "coexpression_transferred" -> IntType,
   "experiments" -> IntType, "experiments_transferred" -> IntType,
   "database" -> IntType, "database_transferred" -> IntType,
  "textmining" -> IntType, "textmining_transferred" -> IntType, "combined_score" -> IntType)

  val nodeType = TupleType("node_protein" -> StringType, "edges" -> BagType(edgeType))

  val netPairType = TupleType("protein1" -> StringType, "protein2" -> StringType, "neighborhood" -> IntType,
   "neighborhood_transferred" -> IntType, "fusion" -> IntType, "cooccurence" -> IntType,
   "homology" -> IntType, "coexpression" -> IntType, "coexpression_transferred" -> IntType,
   "experiments" -> IntType, "experiments_transferred" -> IntType,
   "database" -> IntType, "database_transferred" -> IntType,
  "textmining" -> IntType, "textmining_transferred" -> IntType, "combined_score" -> IntType)
}

trait GeneProteinMap {

  def loadGeneProteinMap(shred: Boolean = false, skew: Boolean = false): String = {
  	if (shred){
  		val bmartLoad = if (skew) "(biomart, biomart.empty)" else "biomart"
  		s"""|val bmartLoader = new BiomartLoader(spark)
    			|val biomart = bmartLoader.load("/nfs_qc4/genomics/mart_export.txt")
    			|val IBag_biomart__D = $bmartLoad
    			|IBag_biomart__D.cache
    			|IBag_biomart__D.count""".stripMargin
  	}else if (skew){
  		s"""|val bmartLoader = new BiomartLoader(spark)
    			|val biomart_L = bmartLoader.load("/nfs_qc4/genomics/mart_export.txt")
    			|val biomart = (biomart_L, biomart_L.empty)
    			|biomart.cache
    			|biomart.count""".stripMargin
  	}else{
  		s"""|val bmartLoader = new BiomartLoader(spark)
    			|val biomart = bmartLoader.load("/nfs_qc4/genomics/mart_export.txt")
    			|biomart.cache
    			|biomart.count""".stripMargin
  	}
  }

  val geneProteinOrderedType = List(("gene_stable_id", StringType), ("gene_stable_id_version" -> StringType),
    ("transcript_stable_id_version", StringType), ("protein_stable_id", StringType), 
    ("protein_stable_id_version", StringType), ("gene_start_bp", IntType), ("gene_end_bp", IntType),
    ("transcript_start_bp", IntType), ("transcript_end_bp", IntType), ("biomart_gene_name", StringType))
  
  val geneProteinMapType = TupleType(geneProteinOrderedType.toMap)

}

trait GeneExpression {

  val geneExprType = TupleType("expr_gene" -> StringType, "fpkm" -> DoubleType)
  val sampleExprType = TupleType("expr_sample" -> StringType, "gene_expression" -> BagType(geneExprType))

}

trait CopyNumber {

	def loadCopyNumber(shred: Boolean = false, skew: Boolean = false): String = {
		if (shred){
		val copynumLoad = if (skew) "(copynumber, copynumber.empty)" else "copynumber"
		s"""|val cnLoader = new CopyNumberLoader(spark)
			  |val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
	    	|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|val IBag_copynumber__D = $copynumLoad
  			|IBag_copynumber__D.cache
  			|IBag_copynumber__D.count""".stripMargin
		}else if (skew)
		s"""|val cnLoader = new CopyNumberLoader(spark)
  			|val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
  			|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|val copynumber = (copynumber_L, copynumber_L.empty)
  			|copynumber.cache
  			|copynumber.count""".stripMargin
		else
		s"""|val cnLoader = new CopyNumberLoader(spark)
		    |val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
    		|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
  			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
  			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]	
  			|copynumber.cache
  			|copynumber.count""".stripMargin

	}
	
	val copyNumberOrderedType = List(("cn_gene_id", StringType), ("cn_gene_name", StringType), ("cn_chromosome", StringType),
		("cn_start", IntType), ("cn_end", IntType), ("cn_copy_number", IntType), ("min_copy_number", IntType),
		("max_copy_number", IntType), ("cn_aliquot_uuid", StringType))

	val copyNumberType = TupleType(copyNumberOrderedType.toMap)

}

trait Biospecimen {

  val biospecOtype = List(
    ("bcr_patient_uuid", StringType), ("bcr_sample_barcode", StringType), 
    ("bcr_aliquot_barcode", StringType), ("bcr_aliquot_uuid", StringType), 
    ("biospecimen_barcode_bottom", StringType), ("center_id", StringType), 
    ("concentration", DoubleType), ("date_of_shipment", StringType), 
    ("is_derived_from_ffpe", StringType), ("plate_column", IntType),
    ("plate_id", StringType), ("plate_row", StringType), 
    ("quantity", DoubleType), ("source_center", IntType), 
    ("volume", DoubleType))
  val biospecType = TupleType(biospecOtype.toMap)

}

trait SOImpact {

	val soImpactType = TupleType("so_term" -> StringType, "so_description" -> StringType, 
			"so_accession" -> StringType, "display_term" -> StringType, "so_impact" -> StringType, "so_weight" -> DoubleType)

}

trait DriverGene extends Query with Occurrence with Gistic with StringNetwork 
  with GeneExpression with Biospecimen with SOImpact with GeneProteinMap with CopyNumber {
  
  val basepath = "/nfs_qc4/genomics/gdc/"
  
  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) loadShred(skew)
    else if (skew) {
    	s"""|val basepath = "/nfs_qc4/genomics/gdc/"
    		|val biospecLoader = new BiospecLoader(spark)
			|val biospec_L = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/")
			|val biospec = (biospec_L, biospec_L.empty)
			|biospec.cache
			|biospec.count
			|val consequenceLoader = new ConsequenceLoader(spark)
			|val consequences_L = consequenceLoader.loadSequential("/nfs_qc4/genomics/calc_variant_conseq.txt")
			|val consequences = (consequences_L, consequences_L.empty)
			|consequences.cache
			|consequences.count
			|${loadOccurrence(shred, skew)}
			|""".stripMargin
	}else{
		s"""|val basepath = "/nfs_qc4/genomics/gdc/"
			|val biospecLoader = new BiospecLoader(spark)
			|val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/")
			|biospec.cache
			|biospec.count
			|val consequenceLoader = new ConsequenceLoader(spark)
			|val consequences = consequenceLoader.loadSequential("/nfs_qc4/genomics/calc_variant_conseq.txt")
			|consequences.cache
			|consequences.count
			|${loadOccurrence(shred, skew)}
			|""".stripMargin
  	}
  }

  def loadShred(skew: Boolean = false): String = {
  	val biospecLoad = if (skew) "(biospec, biospec.empty)" else "biospec"
  	val conseqLoad = if (skew) "(conseq, conseq.empty)" else "conseq"
	s"""|val basepath = "/nfs_qc4/genomics/gdc/"
		|val biospecLoader = new BiospecLoader(spark)
		|val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/")
		|val IBag_biospec__D = $biospecLoad
		|IBag_biospec__D.cache
		|IBag_biospec__D.count
		|val consequenceLoader = new ConsequenceLoader(spark)
		|val conseq = consequenceLoader.loadSequential("/nfs_qc4/genomics/calc_variant_conseq.txt")
		|val IBag_consequences__D = $conseqLoad
		|IBag_consequences__D.cache
		|IBag_consequences__D.count
		|${loadOccurrence(true, skew)}
		|""".stripMargin
  }

  val occurrences = BagVarRef("occurrences", BagType(occurrence_type))
  val or = TupleVarRef("o", occurrence_type)
  val ar = TupleVarRef("a", transcriptQuant)
  val cr = TupleVarRef("c", element)

  val occurmids = BagVarRef("occurrences", BagType(occurmid_type))
  val omr = TupleVarRef("om", occurmid_type)
  val amr = TupleVarRef("am", transcriptFull)

  val gistic = BagVarRef("gistic", BagType(gisticType))
  val gr = TupleVarRef("g", gisticType)
  val sr = TupleVarRef("s", sampleType)

  val network = BagVarRef("network", BagType(nodeType))
  val nr = TupleVarRef("n", nodeType)
  val er = TupleVarRef("e", edgeType)
  val fnetwork = BagVarRef("stringNetwork", BagType(netPairType))
  val fnr = TupleVarRef("fn", netPairType)

  val expression = BagVarRef("expression", BagType(sampleExprType))
  val sexpr = TupleVarRef("sexpr", sampleExprType)
  val gexpr = TupleVarRef("gexpr", geneExprType)

  val biospec = BagVarRef("biospec", BagType(biospecType))
  val br = TupleVarRef("b", biospecType)

  val conseq = BagVarRef("consequences", BagType(soImpactType))
  val conr = TupleVarRef("cons", soImpactType)

  val gpmap = BagVarRef("biomart", BagType(geneProteinMapType))
  val gpr = TupleVarRef("gp", geneProteinMapType)

  val copynum = BagVarRef("copynumber", BagType(copyNumberType))
  val cnr = TupleVarRef("cn", copyNumberType)

  val matchImpact = NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("HIGH", StringType)),
	                  NumericConst(0.8, DoubleType),
	                  NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODERATE", StringType)),
	                  NumericConst(0.5, DoubleType),
	                    NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("LOW", StringType)),
	                      NumericConst(0.3, DoubleType),
	                      NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODIFIER", StringType)),
	                        NumericConst(0.15, DoubleType),
	                        NumericConst(0.01, DoubleType)))))

  val matchImpactMid = NumericIfThenElse(Cmp(OpEq, amr("impact"), Const("HIGH", StringType)),
	                  NumericConst(0.8, DoubleType),
	                  NumericIfThenElse(Cmp(OpEq, amr("impact"), Const("MODERATE", StringType)),
	                  NumericConst(0.5, DoubleType),
	                    NumericIfThenElse(Cmp(OpEq, amr("impact"), Const("LOW", StringType)),
	                      NumericConst(0.3, DoubleType),
	                      NumericIfThenElse(Cmp(OpEq, amr("impact"), Const("MODIFIER", StringType)),
	                        NumericConst(0.15, DoubleType),
	                        NumericConst(0.01, DoubleType)))))

  val mapCNV = ForeachUnion(cnr, copynum,
		ForeachUnion(br, biospec,
			IfThenElse(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),
				projectTuple(cnr, Map("cn_case_uuid" -> br("bcr_patient_uuid"))))))

  val (cnvCases, cncr) = varset("cnvCases", "cnv", mapCNV)
  

  def projectTuple(tr: TupleVarRef, nbag:Map[String, TupleAttributeExpr], omit: List[String] = Nil): BagExpr =
    Singleton(Tuple(tr.tp.attrTps.withFilter(f =>
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) ++ nbag))

}

// Group occurrences (100k) by mutations (large number of top level tuples)
// aggregate each potential driver gene for each mutation (in transcript_consequences)
// 
// This case will not produce domains, and assumes that each transcript
// is annotated with aliquot identifiers 
// Note: previously "HybridBySample"
object HybridByMutation extends DriverGene {

  // val name = "HybridBySample"
  val name = "HybridByMutation"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(or, occurrences,
    Singleton(Tuple("hybrid_sample" -> or("donorId"), //"hybrid_aliquot" -> or("aliquotId"),
    	"hybrid_project" -> or("projectId"),
    	"hybrid_genes" -> 
        	ReduceByKey(
        		ForeachUnion(ar, BagProject(or, "transcript_consequences"),
        			ForeachUnion(br, biospec,
        				IfThenElse(Cmp(OpEq, ar("aliquot_id"), br("bcr_patient_uuid")),
							ForeachUnion(cnr, copynum,
				       			IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),//ar("aliquot_id")),
				       				Cmp(OpEq, ar("gene_id"), cnr("cn_gene_id"))),
	                      		ForeachUnion(cr, BagProject(ar, "consequence_terms"),
	                        		ForeachUnion(conr, conseq,
	                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
	                          				Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpact 
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))
			,List("hybrid_gene_id"), List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

// hybrid by mutation with more fields persisted in the output
//
// previously HybridPlusBySample
object HybridPlusByMutation extends DriverGene {

  val name = "HybridPlusByMutation"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(or, occurrences,
    Singleton(Tuple("hybrid_sample" -> or("donorId"),
    "hybrid_project" -> or("projectId"),
    //"hybrid_aliquotId" -> or("aliquotId"),
      "hybrid_genes" -> 
          ReduceByKey(
            ForeachUnion(ar, BagProject(or, "transcript_consequences"),
              ForeachUnion(br, biospec,
                IfThenElse(Cmp(OpEq, ar("aliquot_id"), br("bcr_patient_uuid")),
            ForeachUnion(cnr, copynum,
                  IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")), //ar("aliquot_id")),
                        Cmp(OpEq, ar("gene_id"), cnr("cn_gene_id"))),
                          ForeachUnion(cr, BagProject(ar, "consequence_terms"),
                            ForeachUnion(conr, conseq,
                              IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                                  Singleton(Tuple(
                              "hybrid_gene_id" -> ar("gene_id"),
                                "hybrid_transcript_id" -> ar("transcript_id"),
                                "hybrid_protein_start" -> ar("protein_start"),
                                "hybrid_protein_end" -> ar("protein_end"),
                                "hybrid_strand" -> ar("ts_strand"),
                                "hybrid_gene_name" -> cnr("cn_gene_name"),
                                "hybrid_max_copy" -> cnr("max_copy_number"),
                                "hybrid_score" -> 
                                conr("so_weight").asNumeric * matchImpact 
                  * (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))
      ,List("hybrid_gene_id", "hybrid_transcript_id", "hybrid_protein_start", "hybrid_protein_end", 
          "hybrid_strand", "hybrid_gene_name", "hybrid_max_copy"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

// Group occurrences (100k) by sample, aggregating all driver 
// genes across all mutations. 
// 
// Previously "HybridBySampleV2"
object HybridBySample extends DriverGene {

  val name = "HybridBySample"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(br, biospec,
        		Singleton(Tuple("hybrid_sample" -> br("bcr_patient_uuid"), 
        			"hybrid_aliquot" -> br("bcr_aliquot_uuid"),
        			"hybrid_center" -> br("center_id"),
        			"hybrid_genes" -> ReduceByKey(
        				ForeachUnion(or, occurrences,
        				IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
			        		ForeachUnion(ar, BagProject(or, "transcript_consequences"),
								ForeachUnion(cncr, cnvCases,
							       	IfThenElse(And(Cmp(OpEq, cncr("cn_case_uuid"), ar("aliquot_id")),
							       		Cmp(OpEq, ar("gene_id"), cncr("cn_gene_id"))),
				                      		ForeachUnion(cr, BagProject(ar, "consequence_terms"),
				                        		ForeachUnion(conr, conseq,
				                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
				                          				Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
					                            		"hybrid_score" -> 
					                            		conr("so_weight").asNumeric * matchImpact 
														* (cncr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}

// Group occurrences by mutation (500k)
// aggregate driver genes for each mutation for a given sample
//
// previously "HybridBySampleMid"
object HybridByMutationMid extends DriverGene {

  val name = "HybridByMutationMid"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(omr, occurmids,
    Singleton(Tuple("hybrid_sample" -> omr("donorId"), //"hybrid_aliquot" -> or("aliquotId"),
    	"hybrid_project" -> omr("projectId"),
    	"hybrid_genes" -> 
        	ReduceByKey(
        		ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
        			ForeachUnion(br, biospec,
        				IfThenElse(Cmp(OpEq, amr("case_id"), br("bcr_patient_uuid")),
							ForeachUnion(cnr, copynum,
				       			IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),//ar("aliquot_id")),
				       				Cmp(OpEq, amr("gene_id"), cnr("cn_gene_id"))),
	                      		ForeachUnion(cr, BagProject(amr, "consequence_terms"),
	                        		ForeachUnion(conr, conseq,
	                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
	                          				Singleton(Tuple("hybrid_gene_id" -> amr("gene_id"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpactMid
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

// group occurrences by sample (500k)
// aggregate driver genes across all mutations for a given sample
//
// previously "HybridBySampleMidV2"
object HybridBySampleMid extends DriverGene {

  val name = "HybridBySampleMid"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(br, biospec,
        		Singleton(Tuple("hybrid_sample" -> br("bcr_patient_uuid"), 
        			"hybrid_aliqout" -> br("bcr_aliquot_uuid"),
        			"hybrid_center" -> br("center_id"),
        			"hybrid_genes" -> ReduceByKey(
        				ForeachUnion(omr, occurmids,
        				IfThenElse(Cmp(OpEq, omr("donorId"), br("bcr_patient_uuid")),
        				ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
							ForeachUnion(cncr, cnvCases,
							     IfThenElse(And(Cmp(OpEq, cncr("cn_case_uuid"), amr("case_id")),
							       		Cmp(OpEq, amr("gene_id"), cncr("cn_gene_id"))),
	                      		ForeachUnion(cr, BagProject(amr, "consequence_terms"),
	                        		ForeachUnion(conr, conseq,
	                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
	                          				Singleton(Tuple("hybrid_gene_id" -> amr("gene_id"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpactMid
											* (cncr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}

// group occurrences (500k) by mutations
// for each mutation within each sample, aggregate the driver genes 
// 
// previously "HybridBySampleMid2"
object HybridByMutationMid2 extends DriverGene {

  val name = "HybridByMutationMid2"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val siftImpact = NumericIfThenElse(Cmp(OpEq, amr("sift_score"), Const(0.0, DoubleType)),
  						NumericConst(0.01, DoubleType), amr("sift_score").asNumeric)

  val polyImpact = NumericIfThenElse(Cmp(OpEq, amr("polyphen_score"), Const(0.0, DoubleType)),
  						NumericConst(0.01, DoubleType), amr("polyphen_score").asNumeric)

  val query = ForeachUnion(omr, occurmids,
    Singleton(Tuple("hybrid_sample" -> omr("donorId"), //"hybrid_aliquot" -> or("aliquotId"),
    	"hybrid_project" -> omr("projectId"),
    	"hybrid_genes" -> 
        	ReduceByKey(
        		ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
        			ForeachUnion(br, biospec,
        				IfThenElse(Cmp(OpEq, amr("case_id"), br("bcr_patient_uuid")),
							ForeachUnion(cnr, copynum,
				       			IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),//ar("aliquot_id")),
				       				Cmp(OpEq, amr("gene_id"), cnr("cn_gene_id"))),
	                      		ForeachUnion(cr, BagProject(amr, "consequence_terms"),
	                        		ForeachUnion(conr, conseq,
	                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
	                          				Singleton(Tuple(
	                          				"hybrid_gene_id" -> amr("gene_id"),
	                          				"hybrid_impact" -> amr("impact"),
	                          				"hybrid_sift" -> amr("sift_prediction"),
	                          				"hybrid_polyphen" -> amr("polyphen_prediction"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpactMid * siftImpact * polyImpact
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))

			,List("hybrid_gene_id", "hybrid_impact", "hybrid_sift", "hybrid_polyphen"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

// group occurrences (500k) by sample
// aggregate driver genes for all mutations across a sample
//
// previously "HybridBySampleMid2V2"
object HybridBySampleMid2 extends DriverGene {

  val name = "HybridBySampleMid2"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val siftImpact = NumericIfThenElse(Cmp(OpEq, amr("sift_score"), Const(0.0, DoubleType)),
  						NumericConst(0.01, DoubleType), amr("sift_score").asNumeric)

  val polyImpact = NumericIfThenElse(Cmp(OpEq, amr("polyphen_score"), Const(0.0, DoubleType)),
  						NumericConst(0.01, DoubleType), amr("polyphen_score").asNumeric)

  val query = ForeachUnion(br, biospec,
        		Singleton(Tuple("hybrid_sample" -> br("bcr_patient_uuid"), 
        			"hybrid_aliquot" -> br("bcr_aliquot_uuid"),
        			"hybrid_center" -> br("center_id"),
        			"hybrid_genes" -> ReduceByKey(ForeachUnion(omr, occurmids,
        				IfThenElse(Cmp(OpEq, omr("donorId"), br("bcr_patient_uuid")),
        					ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
							ForeachUnion(cncr, cnvCases,
							     IfThenElse(And(Cmp(OpEq, cncr("cn_case_uuid"), amr("case_id")),
							       		Cmp(OpEq, amr("gene_id"), cncr("cn_gene_id"))),
	                      		ForeachUnion(cr, BagProject(amr, "consequence_terms"),
	                        		ForeachUnion(conr, conseq,
	                        			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
	                          				Singleton(Tuple(
	                          				"hybrid_gene_id" -> amr("gene_id"),
	                          				"hybrid_impact" -> amr("impact"),
	                          				"hybrid_sift" -> amr("sift_prediction"),
	                          				"hybrid_polyphen" -> amr("polyphen_prediction"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpactMid * siftImpact * polyImpact
											* (cncr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))))

			,List("hybrid_gene_id", "hybrid_impact", "hybrid_sift", "hybrid_polyphen"),
            List("hybrid_score")))))

  val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}

object SampleNetwork extends DriverGene {

  val name = "SampleNetwork"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_aliquot" -> hmr("hybrid_aliquot"),
      	"network_center" -> hmr("hybrid_center"), 
          "network_genes" ->
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(nr, network, 
                ForeachUnion(er, BagProject(nr, "edges"),
                  IfThenElse(Cmp(OpEq, er("edge_protein"), gene("hybrid_gene_id")),
                    Singleton(Tuple("network_gene_id" -> gene("hybrid_gene_id"), 
                      "distance" -> er("combined_score").asNumeric * gene("hybrid_score").asNumeric)))))),
            List("network_gene_id"),
            List("distance")))))

  val program = HybridBySample.program.asInstanceOf[SampleNetwork.Program].append(Assignment(name, query))

}

// do this for Mid and Mid2
object SampleNetworkMid2 extends DriverGene {

  val name = "SampleNetworkMid2"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadNetwork(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |""".stripMargin

  val (hybrid, hmr) = varset(HybridBySampleMid2.name, "hm", HybridBySampleMid2.program(HybridBySampleMid2.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val martMap = gpr.tp.attrTps.map(f => f._1 -> Project(gpr, f._1))
  val gpr2 = TupleVarRef("gp2", gpr.tp)
  val martMap2 = gpr2.tp.attrTps.map(f => s"edge_${f._1}" -> Project(gpr2, f._1))

  val edgeJoin = ForeachUnion(er, BagProject(nr, "edges"), 
      ForeachUnion(gpr2, gpmap,
        IfThenElse(Cmp(OpEq, er("edge_protein"), gpr2("protein_stable_id")),
          projectTuple(er, martMap2))))

  val mappedEdges = Map("node_edges" -> edgeJoin)

  val mappedNetwork = ForeachUnion(nr, network,
      ForeachUnion(gpr, gpmap,
        IfThenElse(Cmp(OpEq, nr("node_protein"), gpr("protein_stable_id")),
          projectTuple(nr, martMap ++ mappedEdges, List("edges"))
        )))

  val (mNet, mnr) = varset("mappedNetwork", "mn", mappedNetwork)
  val mer = TupleVarRef("me", edgeJoin.tp.tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_aliquot" -> hmr("hybrid_aliquot"),
        "network_center" -> hmr("hybrid_center"), 
          "network_genes" ->
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(mnr, mNet, 
                ForeachUnion(mer, BagProject(mnr, "node_edges"),
                  IfThenElse(Cmp(OpEq, mer("edge_gene_stable_id"), gene("hybrid_gene_id")),
                    Singleton(Tuple("network_gene_id" -> gene("hybrid_gene_id"), 
                      "distance" -> mer("combined_score").asNumeric * gene("hybrid_score").asNumeric)))))),
            List("network_gene_id"),
            List("distance")))))

  val program = HybridBySampleMid2.program.asInstanceOf[SampleNetworkMid2.Program].append(Assignment(mNet.name, mappedNetwork)).append(Assignment(name, query))

}

object SampleFlatNetwork extends DriverGene {

  val name = "SampleFlatNetwork"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"""|${super.loadTables(shred, skew)}
  		  |${loadCopyNumber(shred, skew)}
  		  |${loadFlatNetwork(shred, skew)}
  		  |${loadGeneProteinMap(shred, skew)}""".stripMargin

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), //"network_aliquot" -> hmr("hybrid_aliquot"),
      	"network_project" -> hmr("hybrid_project"), 
      	"network_genes" -> 
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(gpr, gpmap,
              	IfThenElse(Cmp(OpEq, gene("hybrid_gene_id"), gpr("gene_stable_id")),
              		ForeachUnion(fnr, fnetwork, 
                		IfThenElse(Cmp(OpEq, fnr("protein2"), gpr("protein_stable_id")),
                    		Singleton(Tuple(
                    			"network_gene_id2" -> gene("hybrid_gene_id"), 
                    			"network_protein_id1" -> fnr("protein1"),
                    			"network_protein_id2" -> fnr("protein2"),
                      			"distance" -> fnr("combined_score").asNumeric * gene("hybrid_score").asNumeric))))))),
            List("network_gene_id2", "network_protein_id1", "network_protein_id2"),
            List("distance")))))

  val program = HybridBySample.program.asInstanceOf[SampleFlatNetwork.Program].append(Assignment(name, query))

}

object EffectBySample extends DriverGene {

  val name = "EffectBySample"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)
  val (snetwork, snr) = varset(SampleNetwork.name, "sn", SampleNetwork.program(SampleNetwork.name).varRef.asInstanceOf[BagExpr])
  val gene2 = TupleVarRef("ngene", snr.tp("network_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid,
      ForeachUnion(snr, snetwork, 
        IfThenElse(Cmp(OpEq, hmr("hybrid_sample"), snr("network_sample")),
          Singleton(Tuple("effect_sample" -> hmr("hybrid_sample"), "effect_genes" -> 
            ForeachUnion(gene1, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(gene2, BagProject(snr, "network_genes"),
                IfThenElse(Cmp(OpEq, gene1("hybrid_gene_id"), gene2("network_gene_id")),
                  Singleton(Tuple("effect_gene_id" -> gene1("hybrid_gene_id"), 
                    "effect" -> gene1("hybrid_score").asNumeric * gene2("distance").asNumeric))))))))))

  val program = SampleNetwork.program.asInstanceOf[EffectBySample.Program].append(Assignment(name, query))

}

object EffectBySample2 extends DriverGene {

  val name = "EffectBySample2"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)
  val (snetwork, snr) = varset(SampleNetwork.name, "sn", SampleNetwork.program(SampleNetwork.name).varRef.asInstanceOf[BagExpr])
  val gene2 = TupleVarRef("ngene", snr.tp("network_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid,
      Singleton(Tuple("effect_sample" -> hmr("hybrid_sample"), "effect_genes" -> 
        ForeachUnion(snr, snetwork, 
          IfThenElse(Cmp(OpEq, hmr("hybrid_sample"), snr("network_sample")),
            ForeachUnion(gene1, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(gene2, BagProject(snr, "network_genes"),
                IfThenElse(Cmp(OpEq, gene1("hybrid_gene_id"), gene2("network_gene_id")),
                  Singleton(Tuple("effect_gene_id" -> gene1("hybrid_gene_id"), 
                    "effect" -> gene1("hybrid_score").asNumeric * gene2("distance").asNumeric))))))))))

  val program = SampleNetwork.program.asInstanceOf[EffectBySample2.Program].append(Assignment(name, query))

}

object ConnectionBySample extends DriverGene {

  val name = "ConnectionBySample"

  val (effect, emr) = varset(EffectBySample.name, "em", EffectBySample.program(EffectBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("egene", emr.tp("effect_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(emr, effect, 
    ForeachUnion(sexpr, expression, 
      IfThenElse(Cmp(OpEq, emr("effect_sample"), sexpr("expr_sample")), 
        Singleton(Tuple("connection_sample" -> emr("effect_sample"), "connect_genes" -> 
          ForeachUnion(gene1, BagProject(emr, "effect_genes"),
            ForeachUnion(gexpr, BagProject(sexpr, "gene_expression"),
              IfThenElse(Cmp(OpEq, gene1("effect_gene_id"), gexpr("expr_gene")),
                Singleton(Tuple("connect_gene" -> gene1("effect_gene_id"), 
                  "gene_connectivity" -> gene1("effect").asNumeric * gexpr("fpkm").asNumeric))))))))))

  val program = EffectBySample.program.asInstanceOf[ConnectionBySample.Program].append(Assignment(name, query))

}

object GeneConnectivity extends DriverGene {

  val name = "GeneConnectivity"

  val (connect, cmr) = varset(ConnectionBySample.name, "em", ConnectionBySample.program(ConnectionBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("cgene", cmr.tp("connect_genes").asInstanceOf[BagType].tp)

  val query = ReduceByKey(ForeachUnion(cmr, connect, 
      ForeachUnion(gene1, BagProject(cmr, "connect_genes"),
        Singleton(Tuple("gene_id" -> gene1("connect_gene"), 
          "connectivity" -> gene1("gene_connectivity"))))),
        List("gene_id"),
        List("connectivity"))

  val program = ConnectionBySample.program.asInstanceOf[GeneConnectivity.Program].append(Assignment(name, query))

}





