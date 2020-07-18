package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait Occurrence extends Vep {

  val occurrence_type = TupleType(
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
      "transcript_consequences" -> BagType(transcriptQuant)
    )

}

trait Gistic {

  val sampleType = TupleType("gistic_sample" -> StringType, 
    "focal_score" -> LongType)

  val gisticType = TupleType("gistic_gene" -> StringType, "gistic_gene_iso" -> StringType, 
  	"cytoband" -> StringType, "gistic_samples" -> BagType(sampleType))

}

trait StringNetwork {

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

}

trait GeneProteinMap {

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
			|val mafLoader = new MAFLoader(spark)
			|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
      |val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
			|val vepLoader = new VepLoader(spark)
			|val (occurs, annots) = vepLoader.loadOccurrences(maf)
			| val occurrences_L = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))
			|//val occurrences_L = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
			|val occurrences = (occurrences_L, occurrences_L.empty)
			|occurrences.cache
			|occurrences.count
			|/**
      |//val gistic_L = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
			|//					.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
			|val gisticLoader = new GisticLoader(spark)
			|val gistic_L = gisticLoader.merge(s"$basepath/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
			|                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
			|val gistic = (gistic_L, gistic_L.empty)
			|gistic.cache
			|gistic.count**/
			|val cnLoader = new CopyNumberLoader(spark)
			|//val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
			|val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
			|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
			|//val copynumber_L = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
			|val copynumber = (copynumber_L, copynumber_L.empty)
			|copynumber.cache
			|copynumber.count
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
			|""".stripMargin
	}else{
		s"""|val basepath = "/nfs_qc4/genomics/gdc/"
			|val mafLoader = new MAFLoader(spark)
      |//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
      |val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
			|val vepLoader = new VepLoader(spark)
			|val (occurs, annots) = vepLoader.loadOccurrences(maf)
			|val occurrences = vepLoader.finalize(vepLoader.buildOccurrences(occurs, annots))
			|//val occurrences = vepLoader.finalize(spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
			|occurrences.cache
			|occurrences.count
			|/**
      |//val gistic = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
			|//				.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
			|val gisticLoader = new GisticLoader(spark)
			|val gistic = gisticLoader.merge(s"$basepath/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
			|                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]				
			|gistic.cache
			|gistic.count**/
			|val cnLoader = new CopyNumberLoader(spark)
			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
      		|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
			|val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
			|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
			|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
			|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]	
			|copynumber.cache
			|copynumber.count
			|val biospecLoader = new BiospecLoader(spark)
			|val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/aliquot/")
			|biospec.cache
			|biospec.count
			|val consequenceLoader = new ConsequenceLoader(spark)
			|val consequences = consequenceLoader.loadSequential("/nfs_qc4/genomics/calc_variant_conseq.txt")
			|consequences.cache
			|consequences.count
			|""".stripMargin
  	}
  }

  def loadShred(skew: Boolean = false): String = {
  	val loadFun = if (skew) "shredSkew" else "shred"
  	val biospecLoad = if (skew) "(biospec, biospec.empty)" else "biospec"
  	val conseqLoad = if (skew) "(conseq, conseq.empty)" else "conseq"
    val copynumLoad = if (skew) "(copynumber, copynumber.empty)" else "copynumber"
	s"""|val basepath = "/nfs_qc4/genomics/gdc/"
		|val mafLoader = new MAFLoader(spark)
		|//val maf = mafLoader.loadFlat(s"$basepath/somatic/small.maf")
    |val maf = mafLoader.loadFlat(s"$basepath/somatic/TCGA.BRCA.mutect.995c0111-d90b-4140-bee7-3845436c3b42.DR-10.0.somatic.maf")
		|val vepLoader = new VepLoader(spark)
		|val (occurs, annots) = vepLoader.loadOccurrences(maf)
		|val occurrences = vepLoader.buildOccurrences(occurs, annots)
		|val (odict1, odict2, odict3) = vepLoader.$loadFun(occurrences)
		|//val (odict1, odict2, odict3) = vepLoader.$loadFun(spark.read.json(
		|//	"file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence])
  	|val IBag_occurrences__D = odict1
		|IBag_occurrences__D.cache
		|IBag_occurrences__D.count
		|val IDict_occurrences__D_transcript_consequences = odict2
		|IDict_occurrences__D_transcript_consequences.cache
		|IDict_occurrences__D_transcript_consequences.count
		|val IDict_occurrences__D_transcript_consequences_consequence_terms = odict3
		|IDict_occurrences__D_transcript_consequences_consequence_terms.cache
		|IDict_occurrences__D_transcript_consequences_consequence_terms.count
		|/**
    |val gisticLoader = new GisticLoader(spark)
		|val gistic = gisticLoader.merge(s"$basepath/gistic/small.txt", dir = false)//BRCA.focal_score_by_genes.txt", dir = false)
		|                .withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]				
		|//val gistic = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
		|//				.withColumn("gistic_gene", substring(col("gistic_gene_iso"), 1,15)).as[Gistic]
		|val (gdict1, gdict2) = gisticLoader.$loadFun(gistic)
		|val IBag_gistic__D = gdict1
		|IBag_gistic__D.cache
		|IBag_gistic__D.count
		|val IDict_gistic__D_gistic_samples = gdict2
		|IDict_gistic__D_gistic_samples.cache
		|IDict_gistic__D_gistic_samples.count**/
		|val cnLoader = new CopyNumberLoader(spark)
		|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/", true)
    	|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
		|val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/brca/", true)
		|				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
		|//val copynumber = cnLoader.load("/nfs_qc4/genomics/gdc/gene_level/TCGA-BRCA.05936306-3484-48d1-9305-f4596aed82f3.gene_level_copy_number.tsv", false)
		|//				.withColumn("cn_gene_id", substring(col("cn_gene_id"), 1,15)).as[CopyNumber]
		|val IBag_copynumber__D = $copynumLoad
		|IBag_copynumber__D.cache
		|IBag_copynumber__D.count
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
		|""".stripMargin
  }

  val occurrences = BagVarRef("occurrences", BagType(occurrence_type))
  val or = TupleVarRef("o", occurrence_type)
  val ar = TupleVarRef("a", transcriptQuant)
  val cr = TupleVarRef("c", element)

  val gistic = BagVarRef("gistic", BagType(gisticType))
  val gr = TupleVarRef("g", gisticType)
  val sr = TupleVarRef("s", sampleType)

  val network = BagVarRef("network", BagType(nodeType))
  val nr = TupleVarRef("n", nodeType)
  val er = TupleVarRef("e", edgeType)

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

object HybridBySampleStandard extends DriverGene {

  val name = "HybridBySample"

  val matchImpact = NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("HIGH", StringType)),
		              NumericConst(0.8, DoubleType),
		              NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODERATE", StringType)),
		             	NumericConst(0.5, DoubleType),
		                NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("LOW", StringType)),
		                  NumericConst(0.3, DoubleType),
		                  NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODIFIER", StringType)),
		                    NumericConst(0.15, DoubleType),
		                    NumericConst(0.01, DoubleType)))))

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
		                          			"hybrid_transcript_id" -> ar("transcript_id"),
		                          			"hyrbid_protein_start" -> ar("protein_start"),
		                          			"hybrid_protein_end" -> ar("protein_end"),
		                          			"hybrid_strand" -> ar("strand"),
		                          			"hybrid_gene_name" -> cnr("cn_gene_name"),
		                          			"hybrid_max_copy" -> cnr("max_copy_number"),
		                          			"hybrid_impact" -> or("most_severe_consequence"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpact 
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType))))))))))
		                          		/**Some(Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> NumericConst(.01, DoubleType) 
											* matchImpact * sr("focal_score").asNumeric)))))))))))**/

			,List("hybrid_gene_id", "hybrid_transcript_id", "hybrid_protein_start", "hybrid_protein_end", 
					"hybrid_strand", "hybrid_gene_name", "hybrid_max_copy", "hybrid_impact"),
            List("hybrid_score")))))))

  val program = Program(Assignment(name, query))

}

object HybridBySample extends DriverGene {

  val name = "HybridBySample"

  val matchImpact = NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("HIGH", StringType)),
		              NumericConst(0.8, DoubleType),
		              NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODERATE", StringType)),
		             	NumericConst(0.5, DoubleType),
		                NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("LOW", StringType)),
		                  NumericConst(0.3, DoubleType),
		                  NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODIFIER", StringType)),
		                    NumericConst(0.15, DoubleType),
		                    NumericConst(0.01, DoubleType)))))

  val query = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
    	Singleton(Tuple("hybrid_sample" -> or("donorId"), 
    		"hybrid_aliquot" -> br("bcr_aliquot_uuid"),
    		"hybrid_project" -> or("projectId"),
        	"hybrid_genes" -> 
	        	ReduceByKey(
	            	ForeachUnion(cnr, copynum,
					       IfThenElse(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")),
					  	    ForeachUnion(ar, BagProject(or, "transcript_consequences"),
					  		     IfThenElse(Cmp(OpEq, ar("gene_id"), cnr("cn_gene_id")),
		                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
		                        ForeachUnion(conr, conseq,
		                        	IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
		                          		Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                          			"hybrid_transcript_id" -> ar("transcript_id"),
		                          			"hyrbid_protein_start" -> ar("protein_start"),
		                          			"hybrid_protein_end" -> ar("protein_end"),
		                          			"hybrid_strand" -> ar("strand"),
		                          			"hybrid_gene_name" -> cnr("cn_gene_name"),
		                          			"hybrid_max_copy" -> cnr("max_copy_number"),
		                          			"hybrid_impact" -> or("most_severe_consequence"),
		                            		"hybrid_score" -> 
		                            		conr("so_weight").asNumeric * matchImpact 
											* (cnr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType)))))))))))
		                          		/**Some(Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
		                            		"hybrid_score" -> NumericConst(.01, DoubleType) 
											* matchImpact * sr("focal_score").asNumeric)))))))))))**/

			,List("hybrid_gene_id", "hybrid_transcript_id", "hybrid_protein_start", "hybrid_protein_end", 
					"hybrid_strand", "hybrid_gene_name", "hybrid_max_copy", "hybrid_impact"),
            List("hybrid_score")))))))

  val program = Program(Assignment(name, query))

}

object ValidateOccurrence extends DriverGene {

	val name = "ValidateOccurrence"
	
	val query = ForeachUnion(or, occurrences, 
		projectTuple(or, Map("transcript_consequences" -> ForeachUnion(ar, BagProject(or, "transcript_consequences"), 
		  projectTuple(ar, Map.empty[String, TupleAttributeExpr])))))
	
	val program = Program(Assignment(name, query))

}

object StandardHybridBySample extends DriverGene {

  val name = "HybridBySample"

  val matchImpact = NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("HIGH", StringType)),
                  NumericConst(0.8, DoubleType),
                  NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODERATE", StringType)),
                  NumericConst(0.5, DoubleType),
                    NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("LOW", StringType)),
                      NumericConst(0.3, DoubleType),
                      NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODIFIER", StringType)),
                        NumericConst(0.15, DoubleType),
                        NumericConst(0.01, DoubleType)))))

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
                                  /**Some(Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
                                    "hybrid_score" -> NumericConst(.01, DoubleType) 
                      * matchImpact * sr("focal_score").asNumeric)))))))))))**/

      ,List("hybrid_gene_id"),
            List("hybrid_score")))))))

  val program = Program(Assignment(gistFlat.name, flattenGistic), Assignment(name, query))

}

object HybridBySampleGistic extends DriverGene {

  val name = "HybridBySample"

  val matchImpact = NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("HIGH", StringType)),
		              NumericConst(0.8, DoubleType),
		              NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODERATE", StringType)),
		             	NumericConst(0.5, DoubleType),
		                NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("LOW", StringType)),
		                  NumericConst(0.3, DoubleType),
		                  NumericIfThenElse(Cmp(OpEq, ar("impact"), Const("MODIFIER", StringType)),
		                    NumericConst(0.15, DoubleType),
		                    NumericConst(0.01, DoubleType)))))

  val query = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
    	Singleton(Tuple("hybrid_sample" -> or("donorId"), "hybrid_aliquot" -> br("bcr_aliquot_uuid"),
        	"hybrid_genes" -> 
	        	ReduceByKey(
	            	ForeachUnion(ar, BagProject(or, "transcript_consequences"),
		              ForeachUnion(gr, gistic, 
		                IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
		                  ForeachUnion(sr, BagProject(gr, "gistic_samples"),
		                    IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
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
            List("hybrid_score")))))))

  val program = Program(Assignment(name, query))

}

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

object SampleNetwork extends DriverGene {

  val name = "SampleNetwork"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_genes" -> 
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(nr, network, 
                ForeachUnion(er, BagProject(nr, "edges"),
                  IfThenElse(Cmp(OpEq, er("edge_protein"), gene("hybrid_gene_id")),
                    Singleton(Tuple("network_gene_id" -> gene("hybrid_gene_id"), 
                      "distance" -> er("combined_score").asNumeric * gene("hybrid_score").asNumeric
                      )))))),
            List("network_gene_id"),
            List("distance")))))

  val program = HybridBySample.program.asInstanceOf[SampleNetwork.Program].append(Assignment(name, query))

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







