package framework.examples.genomic

import framework.common._
import framework.examples.Query

/** This file contains additional 
  * queries on the TCGA occurrence endpoint data
  **/

/** Measure the cost of joining the set vs. 
  * maintaining the normalization 
  **/


object BuildOccurrences1 extends DriverGene {

	override def loadTables(shred:Boolean = false, skew:Boolean = false): String = loadMutations(shred, skew)

	val name = "BuildOccurrences"
	val query = ForeachUnion(mr, mutations, 
		ForeachUnion(anr, annotations,
			IfThenElse(Cmp(OpEq, mr("oid"), anr("vid")), 
					projectTuple(mr, Map("transcript_consequences" -> 
						ForeachUnion(tanr, BagProject(anr, "transcript_consequences"),
							projectTuple(tanr, Map("consequence_terms" ->
								ForeachUnion(canr, BagProject(tanr, "consequence_terms"),
									Singleton(Tuple("term" -> canr("element"))))), List("flags"))))))))
							
	val program = Program(Assignment(name, query))

}

object BuildOccurrences2 extends DriverGene {

	override def loadTables(shred:Boolean = false, skew:Boolean = false): String = loadMutations(shred, skew)

	val attrs = mr.tp.attrTps.map(f => f._1 -> mr(s"${f._1}"))
	val name = "BuildOccurrences"
	val query = ForeachUnion(mr, mutations, 
		ForeachUnion(anr, annotations,
			IfThenElse(Cmp(OpEq, mr("oid"), anr("vid")), 
				ForeachUnion(tanr, BagProject(anr, "transcript_consequences"),
					projectTuple(tanr, attrs)))))
							
	val program = Program(Assignment(name, query))

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
            
          /** PrimitiveIfThenElse(
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
                                //  IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                                //    Singleton(Tuple("conseq_term" -> conr("so_term"), 
                                //      "conseq_weight" -> conr("so_weight"))))))
      ))))))))))

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

/** This version assumes transcripts are not annotated with aliquot id
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
      //    "hybrid_strand", "hybrid_gene_name", "hybrid_max_copy", "hybrid_impact"),
            List("hybrid_score")))))))

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
//    s"""|${super.loadTables(shred, skew)}
//      |${loadGistic(shred, skew)}
//      |${loadCopyNumber(shred, skew)}""".stripMargin

//   val (cnFocals, cnfr) = varset("cnFocals", "cnf", CombineGisticCNByGene.query.asInstanceOf[BagExpr])
//   val cnSamples = BagProject(cnfr, "g_samples")
//   val snfr = TupleVarRef("snf", cnSamples.tp.tp)

//   val query = ForeachUnion(cnfr, cnFocals, 
//    Singleton(Tuple("hybrid_gene" -> cnfr("g_gene"), "hybrid_cytoband" -> cnfr("cytoband"), 
//      "hybrid_samples" -> 
//        ReduceByKey(
//          ForeachUnion(snfr, cnSamples,
//            ForeachUnion(br, biospec, 
//              IfThenElse(Cmp(OpEq, snfr("cng_aliquot"), br("bcr_aliquot_uuid")),
//                ForeachUnion(or, occurrences,
//                  IfThenElse(And(Cmp(OpEq, focur("o_case_id"), br("bcr_patient_uuid")),
//                    Cmp(OpEq, focur("t_gene_id"), cnr("cn_gene_id"))),
//                                Singleton(Tuple("hybrid_case_id" -> focur("o_case_id"),
//                                      "hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
//                                      "hybrid_project" -> focur("o_project"),
//                                      "hybrid_score" -> 
//                                      focur("t_conseqs").asNumeric * focur("t_impact").asNumeric
//                        * (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))
//                        * cnr("cn_copy_number").asNumeric
//                      )))))))
//        ,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
//        List("hybrid_score")))))

//   val program = CombineGisticCNByGene.program.asInstanceOf[CombineGisticCNByGene].append(Assignment(name, query))

// }

// object HybridGisticByGeneByAliquot extends DriverGene {

//   val name = "HybridGisticByGeneByAliquot"

//   override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
//    s"${super.loadTables(shred, skew)}\n${loadGistic(shred, skew)}"

//   val query = ForeachUnion(gr, gistic, 
//    Singleton(Tuple("hybrid_gene" -> gr("gistic_gene"), 
//      "hybrid_cytoband" -> gr("cytoband"), 
//      "hybrid_aliquots" -> 
//        ForeachUnion(sr, BagProject(gr, "gistic_samples"),
//        ForeachUnion(br, biospec, 
//          IfThenElse(Cmp(OpEq, sr("gistic_sample"), br("bcr_aliquot_uuid")),
//            Singleton(Tuple("hybrid_aliquot_id" -> br("bcr_aliquot_uuid"), 
//              "hybrid_aliquot_barcode" -> br("bcr_aliquot_barcode"),
//              "hybrid_center" -> br("source_center"),
//              "hybrid_samples" -> 
//                ReduceByKey(ForeachUnion(or, occurrences, 
//                  IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
//                    ForeachUnion(ar, BagProject(or, "transcript_consequences"),
//                      IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
//                        ForeachUnion(cr, BagProject(ar, "consequence_terms"),
//                                    ForeachUnion(conr, conseq,
//                                      IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
//                                          Singleton(Tuple("hybrid_case_id" -> or("donorId"),
//                                            "hybrid_aliquot_id" -> br("bcr_aliquot_uuid"),
//                                            "hybrid_project" -> or("projectId"),
//                                            "hybrid_score" -> 
//                                            conr("so_weight").asNumeric * matchImpact 
//                              * (sr("focal_score").asNumeric + NumericConst(.01, DoubleType))))))))))))))
//        ,List("hybrid_case_id", "hybrid_aliquot_id", "hybrid_project"),
//        List("hybrid_score")))))

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

  val (mNet, mnr) = varset(MappedNetwork.name, "mn", MappedNetwork.query.asInstanceOf[BagExpr])
  val mer = TupleVarRef("me", MappedNetwork.edgeJoin.tp.tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_aliquot" -> hmr("hybrid_aliquot"),
        "network_center" -> hmr("hybrid_center"), 
          "network_genes" ->
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(mnr, mNet, 
                ForeachUnion(mer, BagProject(mnr, "node_edges"),
                  IfThenElse(Cmp(OpEq, mer("edge_gene_stable_id2"), gene("hybrid_gene_id")),
                    Singleton(Tuple("network_gene_id" -> gene("hybrid_gene_id"), 
                      "distance" -> mer("combined_score").asNumeric * gene("hybrid_score").asNumeric)))))),
            List("network_gene_id"),
            List("distance")))))

  val program = HybridBySampleMid2.program.asInstanceOf[SampleNetworkMid2.Program]
    .append(Assignment(MappedNetwork.mart2Rel.name, MappedNetwork.mart2.asInstanceOf[Expr]))
    .append(Assignment(mNet.name, MappedNetwork.query.asInstanceOf[Expr]))
    .append(Assignment(name, query))

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

object EffectBySample0 extends DriverGene {

  val name = "EffectBySample0"

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

  val program = SampleNetwork.program.asInstanceOf[EffectBySample0.Program].append(Assignment(name, query))

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
