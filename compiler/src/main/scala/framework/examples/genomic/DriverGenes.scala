package framework.examples.genomic

import framework.common._
import framework.examples.Query

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
				|/**val mafLoader = new MAFLoader(spark)
				|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
		    	|//val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
				|val (occurs, annots) = vepLoader.loadOccurrences(maf)
				|val occurrences = vepLoader.buildOccurrences(occurs, annots)
				|val (odict1, odict2, odict3) = vepLoader.$loadFun(occurrences)**/
				|//val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence]
				|//val (odict1, odict2, odict3) = vepLoader.$loadFun(vepLoader.finalize(occurrences, biospec))
				|//val (odict1, odict2, odict3) = vepLoader.$loadFun(vepLoader.finalize(occurrences))
				|val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
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
				|/**val mafLoader = new MAFLoader(spark)
				|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
	      		|val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
				|val (occurs, annots) = vepLoader.loadOccurrences(maf)
				|val occurrences_L = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))**/
				|//val occurrences_L = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence], biospec_L)
				|//val occurrences_L = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
				|val occurrences_L = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
				|val occurrences = (occurrences_L, occurrences_L.empty)
				|occurrences.cache
				|occurrences.count
				|""".stripMargin
    	}else{
			s"""|val vepLoader = new VepLoader(spark)
				|/**val mafLoader = new MAFLoader(spark)
	      		|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
	      		|val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
				|val (occurs, annots) = vepLoader.loadOccurrences(maf)
				|//val occurrences = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))**/
				|//val occurrences = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence], biospec)
				|//val occurrences = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
				|val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/datasetFull/").as[OccurrenceMid]
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

  def projectTuple(tr: TupleVarRef, nbag:Map[String, TupleAttributeExpr], omit: List[String] = Nil): BagExpr =
    Singleton(Tuple(tr.tp.attrTps.withFilter(f =>
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) ++ nbag))

}

// fails in shredded
object QuantifyConsequence extends DriverGene {

	val name = "QuantifyConsequence"
	val query = ForeachUnion(or, occurrences, 
		projectTuple(or, Map("tcs" -> ForeachUnion(ar, BagProject(or, "transcript_consequences"),
			projectTuple(ar, Map("cons" -> ForeachUnion(cr, BagProject(ar, "consequence_terms"),
				ForeachUnion(conr, conseq, 
					BagIfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
						Singleton(Tuple("element" -> conr("so_weight"))),
						Some(Singleton(Tuple("element" -> Const(.01, DoubleType)))))))))))))
						
					/**	PrimitiveIfThenElse(
						Cmp(OpEq, conr("so_term"), cr("element")), 
							conr("so_weight").asPrimitive, Const(.01, DoubleType))))))))))))**/

	val program = Program(Assignment(name, query))

}

object OccurGroupByCase0 extends DriverGene {
	val name = "OccurGroupByCase"
	val query = ForeachUnion(br, biospec,
		Singleton(Tuple("o_case_id" -> br("bcr_patient_uuid"), "o_mutations" ->
			ForeachUnion(or, occurrences, 
				IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
					Singleton(Tuple("one" -> or("projectId"), "two" -> or("transcript_consequences"))))))))
					// projectTuple(or, Map("aliquotId" -> br("bcr_aliquot_uuid"))))))))
	
	val program = Program(Assignment(name, query))
}

object OccurGroupByCase1 extends DriverGene {

	val name = "OccurGroupByCase"

	val query = ForeachUnion(br, biospec,
		Singleton(Tuple("o_case_id" -> br("bcr_patient_uuid"), "o_mutations" ->
			ForeachUnion(or, occurrences, 
				IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
					Singleton(Tuple("one" -> or("projectId"), "two" -> 
						ForeachUnion(ar, BagProject(or, "transcript_consequences"),
							Singleton(Tuple("three" -> ar("gene_id"), "four" -> 
								ForeachUnion(cr, BagProject(ar, "consequence_terms"),
									Singleton(Tuple("five" -> cr("element")))
						)))))))))))
	val program = Program(Assignment(name, query))
}

object OccurGroupByCase extends DriverGene {
	val name = "OccurGroupByCase"
	val query = ForeachUnion(br, biospec,
		Singleton(Tuple("o_case_id" -> br("bcr_patient_uuid"), "o_mutations" ->
			ForeachUnion(or, occurrences, 
				IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
					projectTuple(or, Map("aliquotId" -> br("bcr_aliquot_uuid"), 
						"o_trans_conseq" -> ForeachUnion(ar, BagProject(or, "transcript_consequences"),
						projectTuple(ar, Map("o_conseq_terms" -> 
							ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                        ForeachUnion(conr, conseq,
		                        	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                        		Singleton(Tuple("conseq_term" -> conr("so_term"), "conseq_weight" -> conr("so_weight"))))))),
						 List("consequence_terms", "flags"))
					)), List("transcript_consequences"))
			)))))
	val program = Program(Assignment(name, query))
}

object OccurGroupByCaseMid0 extends DriverGene {

	val name = "OccurGroupByCaseMid0"

	val query = ForeachUnion(br, biospec,
		Singleton(Tuple("o_case_id" -> br("bcr_patient_uuid"), "o_mutations" ->
			ForeachUnion(omr, occurmids, 
				IfThenElse(Cmp(OpEq, omr("donorId"), br("bcr_patient_uuid")),
					Singleton(Tuple(//"o_aliquotId" -> br("bcr_aliquot_uuid"), 
						"o_trans_conseq" -> 
						ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
							Singleton(Tuple("o_transcript_id" -> amr("transcript_id"), 
								"o_conseq_terms" -> 
									ForeachUnion(cr, BagProject(amr, "consequence_terms"),
										Singleton(Tuple("o_weight" -> cr("element"))))
		                        		// ForeachUnion(conr, conseq,
		                        		// 	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                        		// 		Singleton(Tuple("conseq_term" -> conr("so_term"), 
		                        		// 			"conseq_weight" -> conr("so_weight"))))))
			))))))))))

	val program = Program(Assignment(name, query))
	
}

object OccurGroupByCaseMid extends DriverGene {

	val name = "OccurGroupByCaseMid"

	val query = ForeachUnion(br, biospec,
		Singleton(Tuple("o_case_id" -> br("bcr_patient_uuid"), "o_mutations" ->
			ForeachUnion(omr, occurmids, 
				IfThenElse(Cmp(OpEq, omr("donorId"), br("bcr_patient_uuid")),
					projectTuple(omr, Map(//"aliquotId" -> br("bcr_aliquot_uuid"), 
						"o_trans_conseq" -> ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
						projectTuple(amr, Map("o_conseq_terms" -> 
							ForeachUnion(cr, BagProject(amr, "consequence_terms"),
		                        ForeachUnion(conr, conseq,
		                        	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                        		Singleton(Tuple("conseq_term" -> conr("so_term"), "conseq_weight" -> conr("so_weight"))))))),
						 List("consequence_terms", "flags"))
					)), List("transcript_consequences")))))))

	val program = Program(Assignment(name, query))
	
}

object OccurGroupByGene extends DriverGene {

	val name = "OccurGroupByGene"

	val one = ForeachUnion(or, occurrences, 
				ForeachUnion(ar, BagProject(or, "transcript_consequences"),
					Singleton(Tuple("g_gene_id" -> ar("gene_id"),
						"g_donorId" -> or("donorId"),
						"g_projectId" -> or("projectId"),
						"g_start" -> or("start"),
						"g_end" -> or("end"),
						"g_strand" -> or("strand"),
						"g_reference" -> or("Reference_Allele"),
						"g_Tumor_Seq_Allele1" -> or("Tumor_Seq_Allele1"),
						"g_Tumor_Seq_Allele2" -> or("Tumor_Seq_Allele2"),
						"g_chrom" -> or("chromosome"),
						"g_distance" -> ar("distance"),
						"g_impact" -> ar("impact"),
						"g_conseqs" -> ar("consequence_terms"),
						"g_transcript_id" -> ar("transcript_id")))))

	val (oneBag, oneRef) = varset("occurFlat", "of", one)

	val query = ForeachUnion(gr, gistic, 
		Singleton(Tuple("g_gene_id" -> gr("gistic_gene"), 
			"g_occurrences" -> ForeachUnion(oneRef, oneBag,
				IfThenElse(Cmp(OpEq, oneRef("g_gene_id"), gr("gistic_gene")), 
					Singleton(oneRef))))))

	val program = Program(Assignment(oneBag.name, one), Assignment(name, query))
}

/** This versions assumes transcripts are not annotated with aliquot id
  * This should create domains for the shredded case.
  */
object HybridBySampleStandard extends DriverGene {

  val name = "HybridBySample"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
    	Singleton(Tuple("hybrid_sample" -> or("donorId"),
    		"hybrid_aliquot" -> br("bcr_aliquot_uuid"),
    		"hybrid_project" -> or("projectId"),
        	"hybrid_genes" -> 
	        	ReduceByKey(
					ForeachUnion(ar, BagProject(or, "transcript_consequences"),
					  ForeachUnion(cnr, copynum,
					    IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),
									   Cmp(OpEq, ar("gene_id"), cnr("cn_gene_id"))),
		                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                        ForeachUnion(conr, conseq,
		                        	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                          		Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                          			// "hybrid_transcript_id" -> ar("transcript_id"),
		                          			// "hyrbid_protein_start" -> ar("protein_start"),
		                          			// "hybrid_protein_end" -> ar("protein_end"),
		                          			// "hybrid_strand" -> ar("strand"),
		                          			// "hybrid_gene_name" -> cnr("cn_gene_name"),
		                          			// "hybrid_max_copy" -> cnr("max_copy_number"),
		                          			// "hybrid_impact" -> or("most_severe_consequence"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpact 
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))
		                          		/**Some(Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> NumericConst(.01, DoubleType) 
											* matchImpact * sr("focal_score").asNumeric)))))))))))**/

			,List("hybrid_gene_id"),
			// "hybrid_transcript_id", "hybrid_protein_start", "hybrid_protein_end", 
			//		"hybrid_strand", "hybrid_gene_name", "hybrid_max_copy", "hybrid_impact"),
            List("hybrid_score")))))))

  val program = Program(Assignment(name, query))

}

/** This case will not produce domains, and assumes that each transcript
  * is annotated with aliquot identifiers **/
object HybridBySample extends DriverGene {

  val name = "HybridBySample"

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

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

object HybridBySampleMid extends DriverGene {

  val name = "HybridBySampleMid"

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

object HybridBySampleMid2 extends DriverGene {

  val name = "HybridBySampleMid2"

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

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

object HybridBySampleNoAgg extends DriverGene {

  val name = "HybridBySampleNoAgg"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(or, occurrences,
    Singleton(Tuple("hybrid_sample" -> or("donorId"), "aliquot_id" -> or("aliquotId"),
    	"hybrid_project" -> or("projectId"),
    	"hybrid_genes" -> 
        	ForeachUnion(ar, BagProject(or, "transcript_consequences"),
				ForeachUnion(cnr, copynum,
				    IfThenElse(And(Cmp(OpEq, cnr("cn_aliquot_uuid"), ar("aliquot_id")),
				       			Cmp(OpEq, ar("gene_id"), cnr("cn_gene_id"))),
				    	ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                    ForeachUnion(conr, conseq,
		                      	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),	
				    	Singleton(Tuple(
				    		"hybrid_distance" -> ar("distance"),
	                          "hybrid_impact" -> ar("impact"),
	                          	"hybrid_gene_id" -> ar("gene_id"),
	                          	"hybrid_transcript_id" -> ar("transcript_id"),
	                          	"hybrid_copy_number" -> (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType)),
	                          	"hybrid_max_copy" -> cnr("max_copy_number"),
	                          	"hybrid_min_copy" -> cnr("min_copy_number"),
	                          	"hybrid_gene_name" -> cnr("cn_gene_name"),
	                          	"hybrid_term" -> conr("so_term"), 
		                        "hybrid_weight" -> conr("so_weight"), 
		                        "hybrid_impact" -> conr("so_impact"))))))))))))

  val program = Program(Assignment(name, query))

}

object HybridPlusBySample extends DriverGene {

  val name = "HybridPlusBySample"

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

object HybridGisticFlatBySample extends DriverGene {

  val name = "HybridGisticFlatBySample"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

  val flattenGistic = ForeachUnion(gr, gistic,  
      ForeachUnion(sr, BagProject(gr, "gistic_samples"),
        Singleton(Tuple("gistic_gene2" -> gr("gistic_gene"), 
          "gistic_sample2" -> sr("gistic_sample"), "gistic_focal" -> sr("focal_score")))))

  val (gistFlat, grf) = varset("GisticFlattened", "gf", flattenGistic)

  val query = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
      Singleton(Tuple("hybrid_sample" -> or("donorId"), "hybrid_aliquot" -> br("bcr_aliquot_uuid"),
          "hybrid_genes" -> 
            ReduceByKey(
                ForeachUnion(grf, gistFlat,
	             	IfThenElse(Cmp(OpEq, grf("gistic_sample2"), br("bcr_aliquot_uuid")),
						ForeachUnion(ar, BagProject(or, "transcript_consequences"),
                    		IfThenElse(Cmp(OpEq, ar("gene_id"), grf("gistic_gene2")),
                          		ForeachUnion(cr, BagProject(ar, "consequence_terms"),
                            		ForeachUnion(conr, conseq,
                              			IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                                  			Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
                                    			"hybrid_score" -> 
                                    				conr("so_weight").asNumeric * matchImpact 
                      								* (grf("gistic_focal").asNumeric + NumericConst(.01, DoubleType)))))))))))

      ,List("hybrid_gene_id"),
            List("hybrid_score")))))))

  val program = Program(Assignment(gistFlat.name, flattenGistic), Assignment(name, query))

}

object HybridGisticBySample extends DriverGene {

  val name = "HybridGisticBySample"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

  val query = ForeachUnion(or, occurrences, 
    	Singleton(Tuple("hybrid_sample" -> or("donorId"), 
    		"hybrid_aliquot" -> or("aliquotId"),
    		"hybrid_project" -> or("projectId"),
        	"hybrid_genes" -> 
	        	ReduceByKey(
	            	ForeachUnion(ar, BagProject(or, "transcript_consequences"),
		              ForeachUnion(gr, gistic, 
		                IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
		                  ForeachUnion(sr, BagProject(gr, "gistic_samples"),
		                    IfThenElse(Cmp(OpEq, sr("gistic_sample"), ar("aliquot_id")),
		                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                        ForeachUnion(conr, conseq,
		                        	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                          		Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpact 
											* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))))))))))))
		                          		/**Some(Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> NumericConst(.01, DoubleType) 
											* matchImpact * sr("focal_score").asNumeric)))))))))))**/

			,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

object HybridGisticByGeneStandard extends DriverGene {

  val name = "HybridGisticByGene"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

  val flatOccur = ForeachUnion(or, occurrences, 
  	ForeachUnion(ar, BagProject(or, "transcript_consequences"),
  		ForeachUnion(cr, BagProject(ar, "consequence_terms"),
  			ForeachUnion(conr, conseq,
		        IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
  					Singleton(Tuple("o_case_id" -> or("donorId"), "o_project" -> or("projectId"),
  						"t_conseqs" -> conr("so_weight"), "t_gene_id" -> ar("gene_id"),
  						"t_impact" -> matchImpact)))))))

  val (fOccurr, focur) = varset("flatOccurr", "focur", flatOccur)

  val query = ForeachUnion(gr, gistic, 
  	Singleton(Tuple("hybrid_gene" -> gr("gistic_gene"), "hybrid_cytoband" -> gr("cytoband"), 
  		"hybrid_samples" -> ReduceByKey(ForeachUnion(sr, BagProject(gr, "gistic_samples"),
  		ForeachUnion(br, biospec, 
  			IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
  				ForeachUnion(focur, fOccurr, 
  					IfThenElse(Cmp(OpEq, focur("o_case_id"), br("bcr_patient_uuid")),
  						IfThenElse(Cmp(OpEq, focur("t_gene_id"), gr("gistic_gene")),
                          		Singleton(Tuple("hybrid_case_id" -> focur("o_case_id"),
                          				"hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
                          				"hybrid_project" -> focur("o_project"),
                            			"hybrid_score" -> 
                            			focur("t_conseqs").asNumeric * focur("t_impact").asNumeric
										* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))))))))))
  			,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
  			List("hybrid_score")))))

  val program = Program(Assignment(fOccurr.name, flatOccur), Assignment(name, query))

}

object HybridGisticByGene extends DriverGene {

  val name = "HybridGisticByGene"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

  val query = ForeachUnion(gr, gistic, 
  	Singleton(Tuple("hybrid_gene" -> gr("gistic_gene"), "hybrid_cytoband" -> gr("cytoband"), 
  		"hybrid_samples" -> ReduceByKey(ForeachUnion(sr, BagProject(gr, "gistic_samples"),
  		ForeachUnion(br, biospec, 
  			IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
  				ForeachUnion(or, occurrences, 
  					IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
  						ForeachUnion(ar, BagProject(or, "transcript_consequences"),
  							IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
  								ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                        	ForeachUnion(conr, conseq,
		                        		IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                          			Singleton(Tuple("hybrid_case_id" -> or("donorId"),
		                          				"hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
		                          				"hybrid_project" -> or("projectId"),
		                            			"hybrid_score" -> 
		                            			conr("so_weight").asNumeric * matchImpact 
												* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))))))))))))))
  			,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
  			List("hybrid_score")))))

  val program = Program(Assignment(name, query))

}

object HybridGisticCNByGene extends DriverGene {

  val name = "HybridGisticCNByGene"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
  	s"""|${super.loadTables(shred, skew)}
  		|${loadGistic(shred, skew)}
  		|${loadCopyNumber(shred, skew)}""".stripMargin

  val flatOccur = ForeachUnion(or, occurrences, 
  	ForeachUnion(ar, BagProject(or, "transcript_consequences"),
  		ForeachUnion(cr, BagProject(ar, "consequence_terms"),
  			ForeachUnion(conr, conseq,
		        IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
  					Singleton(Tuple("o_case_id" -> or("donorId"), "o_project" -> or("projectId"),
  						"t_conseqs" -> conr("so_weight"), "t_gene_id" -> ar("gene_id"),
  						"t_impact" -> matchImpact)))))))

  val (fOccurr, focur) = varset("flatOccurr", "focur", flatOccur)

  val query = ForeachUnion(gr, gistic, 
  	Singleton(Tuple("hybrid_gene" -> gr("gistic_gene"), "hybrid_cytoband" -> gr("cytoband"), 
  		"hybrid_samples" -> ReduceByKey(ForeachUnion(cnr, copynum,
  				IfThenElse(Cmp(OpEq, cnr("cn_gene_id"), gr("gistic_gene")),
  					ForeachUnion(sr, BagProject(gr, "gistic_samples"),
  						IfThenElse(Cmp(OpEq, cnr("cn_aliquot_uuid"), sr("gistic_sample")),
  							ForeachUnion(br, biospec, 
  								IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
  									ForeachUnion(focur, fOccurr,
  										IfThenElse(And(Cmp(OpEq, focur("o_case_id"), br("bcr_patient_uuid")),
  											Cmp(OpEq, focur("t_gene_id"), cnr("cn_gene_id"))),
		                          			Singleton(Tuple("hybrid_case_id" -> focur("o_case_id"),
		                          				"hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
		                          				"hybrid_project" -> focur("o_project"),
		                            			"hybrid_score" -> 
		                            			focur("t_conseqs").asNumeric * focur("t_impact").asNumeric
												* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))
												* cnr("cn_copy_number").asNumeric
											))))))))))
  			,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
  			List("hybrid_score")))))

  val program = Program(Assignment(fOccurr.name, flatOccur), Assignment(name, query))

}

object CombineGisticCNByGene extends DriverGene {
	
	val name = "CombineGisticCNByGene"

	override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
	  	s"""|${loadGistic(shred, skew)}
	  		|${loadCopyNumber(shred, skew)}""".stripMargin

	val query = ForeachUnion(gr, gistic,
		Singleton(Tuple("g_gene" -> gr("gistic_gene"), "g_cytoband" -> gr("cytoband"),
			"g_samples" -> ForeachUnion(sr, BagProject(gr, "gistic_samples"),
				ForeachUnion(cnr, copynum,
					IfThenElse(And(Cmp(OpEq, sr("gistic_sample"), cnr("cn_aliquot_uuid")),
						Cmp(OpEq, gr("gistic_gene"), cnr("cn_gene_id"))),
						Singleton(Tuple("cng_aliquot" -> cnr("cn_aliquot_uuid"), 
							"cng_focal_score" -> sr("focal_score"),
							"cng_copy_number" -> cnr("cn_copy_number"),
							"cng_max_copy" -> cnr("max_copy_number"),
							"cng_min_copy" -> cnr("min_copy_number")))))))))

	val program = Program(Assignment(name, query))

}

// object HybridGisticCNByGene2 extends DriverGene {

//   val name = "HybridGisticCNByGene2"

//   override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
//   	s"""|${super.loadTables(shred, skew)}
//   		|${loadGistic(shred, skew)}
//   		|${loadCopyNumber(shred, skew)}""".stripMargin

//   val (cnFocals, cnfr) = varset("cnFocals", "cnf", CombineGisticCNByGene.query.asInstanceOf[BagExpr])
//   val cnSamples = BagProject(cnfr, "g_samples")
//   val snfr = TupleVarRef("snf", cnSamples.tp.tp)

//   val query = ForeachUnion(cnfr, cnFocals, 
//   	Singleton(Tuple("hybrid_gene" -> cnfr("g_gene"), "hybrid_cytoband" -> cnfr("cytoband"), 
//   		"hybrid_samples" -> 
//   			ReduceByKey(
//   				ForeachUnion(snfr, cnSamples,
//   					ForeachUnion(br, biospec, 
//   						IfThenElse(Cmp(OpEq, snfr("cng_aliquot"), br("bcr_aliquot_uuid")),
//   							ForeachUnion(or, occurrences,
//   								IfThenElse(And(Cmp(OpEq, focur("o_case_id"), br("bcr_patient_uuid")),
//   									Cmp(OpEq, focur("t_gene_id"), cnr("cn_gene_id"))),
// 		                          	Singleton(Tuple("hybrid_case_id" -> focur("o_case_id"),
// 		                          				"hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
// 		                          				"hybrid_project" -> focur("o_project"),
// 		                            			"hybrid_score" -> 
// 		                            			focur("t_conseqs").asNumeric * focur("t_impact").asNumeric
// 												* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))
// 												* cnr("cn_copy_number").asNumeric
// 											)))))))
//   			,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
//   			List("hybrid_score")))))

//   val program = CombineGisticCNByGene.program.asInstanceOf[CombineGisticCNByGene].append(Assignment(name, query))

// }

// object HybridGisticByGeneByAliquot extends DriverGene {

//   val name = "HybridGisticByGeneByAliquot"

//   override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
//   	s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

//   val query = ForeachUnion(gr, gistic, 
//   	Singleton(Tuple("hybrid_gene" -> gr("gistic_gene"), 
//   		"hybrid_cytoband" -> gr("cytoband"), 
//   		"hybrid_aliquots" -> 
//   			ForeachUnion(sr, BagProject(gr, "gistic_samples"),
//   			ForeachUnion(br, biospec, 
//   				IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
//   					Singleton(Tuple("hybrid_aliquot_id" -> br("bcr_aliquot_uuid"), 
//   						"hybrid_aliquot_barcode" -> br("bcr_aliquot_barcode"),
//   						"hybrid_center" -> br("source_center"),
//   						"hybrid_samples" -> 
//   							ReduceByKey(ForeachUnion(or, occurrences, 
//   								IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
//   									ForeachUnion(ar, BagProject(or, "transcript_consequences"),
// 			  							IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
// 			  								ForeachUnion(cr, BagProject(ar, "consequence_terms"),
// 					                        	ForeachUnion(conr, conseq,
// 					                        		IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
// 					                          			Singleton(Tuple("hybrid_case_id" -> or("donorId"),
// 					                          				"hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
// 					                          				"hybrid_project" -> or("projectId"),
// 					                            			"hybrid_score" -> 
// 					                            			conr("so_weight").asNumeric * matchImpact 
// 															* (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))))))))))))))
//   			,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
//   			List("hybrid_score")))))

//   val program = Program(Assignment(name, query))

// }

// fails 
object HybridBySample2 extends DriverGene {

  val name = "HybridBySample2"

  val mapped = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
        projectTuple(or, Map("aliquot_id" -> Project(br, "bcr_aliquot_uuid"))))))
  val (maps, mapsRef) = varset("occurrence_mapped", "mr", mapped)
  
  val query = ForeachUnion(mapsRef, maps,
      Singleton(Tuple("hybrid_sample" -> mapsRef("donorId"), "hybrid_aliqout" -> mapsRef("aliquot_id"),
        "hybrid_genes" -> 
        ReduceByKey(
            ForeachUnion(ar, BagProject(mapsRef, "transcript_consequences"),
              ForeachUnion(gr, gistic, 
                IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
                  ForeachUnion(sr, BagProject(gr, "gistic_samples"),
                    IfThenElse(Cmp(OpEq, sr("gistic_sample"), or("donorId")),
                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
                          Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
                            "hybrid_score" -> sr("focal_score").asNumeric))))))))
            ,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(maps.name, mapped), Assignment(name, query))

}

object SampleSomaticNetwork extends DriverGene {

  val name = "SampleSomaticNetwork"

  val query = ForeachUnion(or, occurrences, 
      Singleton(Tuple("network_sample" -> or("donorId"), "network_genes" -> 
          ReduceByKey(
            ForeachUnion(ar, BagProject(or, "transcript_consequences"),
              ForeachUnion(nr, network, 
                ForeachUnion(er, BagProject(nr, "edges"),
                  IfThenElse(Cmp(OpEq, er("edge_protein"), ar("gene_id")),
                  	ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		            	ForeachUnion(conr, conseq,
		                    IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                    			Singleton(Tuple("network_gene_id" -> ar("gene_id"), 
                      				"distance" -> er("combined_score").asNumeric * 
                      				conr("so_weight").asNumeric * matchImpact ))))))))),
            List("network_gene_id"),
            List("distance")))))

  val program = Program(Assignment(name, query))

}

object SampleNetwork extends DriverGene {

  val name = "SampleNetwork"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), //"network_aliquot" -> hmr("hybrid_aliquot"),
      	"network_project" -> hmr("hybrid_project"), 
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





