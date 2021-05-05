package framework.examples.genomic

import framework.common._
import framework.examples.Query

/** This file contains the queries for the 
  * pipeline analysis of the biomedical benchmark. 
  *
  **/

object HybridBySampleNew extends DriverGene {

  val name = "HybridBySampleNew"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"${super.loadTables(shred, skew)}\n${loadOccurrence(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val siftImpact = NumericIfThenElse(Cmp(OpEq, amr("sift_score"), Const(0.0, DoubleType)),
              NumericConst(0.01, DoubleType), amr("sift_score").asNumeric)

  val polyImpact = NumericIfThenElse(Cmp(OpEq, amr("polyphen_score"), Const(0.0, DoubleType)),
              NumericConst(0.01, DoubleType), amr("polyphen_score").asNumeric)

  val step1Query = ReduceByKey(ForeachUnion(omr, occurmids,
                  ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
              ForeachUnion(cncr, cnvCases,
                   IfThenElse(And(Cmp(OpEq, cncr("cn_case_uuid"), omr("donorId")),
                        Cmp(OpEq, amr("gene_id"), cncr("cn_gene_id"))),
                            ForeachUnion(cr, BagProject(amr, "consequence_terms"),
                              ForeachUnion(conr, conseq,
                                IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                                    Singleton(Tuple(
                                    "hybrid_case_id" -> omr("donorId"),
                                    "hybrid_gene_id1" -> amr("gene_id"),
                                    "hybrid_score1" -> 
                                    conr("so_weight").asNumeric * matchImpactMid * siftImpact * polyImpact
                      * (cncr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType)))))))))))
            ,List("hybrid_case_id", "hybrid_gene_id1"),
            List("hybrid_score1"))
  
  val (step1, s1r) = varset("step1", "s1", step1Query)
  
  val query = ForeachUnion(br, biospec,
            Singleton(Tuple("hybrid_sample" -> br("bcr_patient_uuid"), 
              "hybrid_aliquot" -> br("bcr_aliquot_uuid"),
              "hybrid_center" -> br("center_id"),
              "hybrid_genes" -> ForeachUnion(s1r, step1, 
                  IfThenElse(Cmp(OpEq, s1r("hybrid_case_id"), br("bcr_patient_uuid")),
                    Singleton(Tuple("hybrid_gene_id" -> s1r("hybrid_gene_id1"),
                        "hybrid_score" -> s1r("hybrid_score1")
                      )))))))

  val program = Program(Assignment("cnvCases", mapCNV), Assignment("step1", step1Query), Assignment(name, query))

}

// hybrid score optimized for shredding
object HybridBySampleNewS extends DriverGene {

  val name = "HybridBySampleNewS"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"${super.loadTables(shred, skew)}\n${loadOccurrence(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val siftImpact = NumericIfThenElse(Cmp(OpEq, amr("sift_score"), Const(0.0, DoubleType)),
              NumericConst(0.01, DoubleType), amr("sift_score").asNumeric)

  val polyImpact = NumericIfThenElse(Cmp(OpEq, amr("polyphen_score"), Const(0.0, DoubleType)),
              NumericConst(0.01, DoubleType), amr("polyphen_score").asNumeric)

  val step0Query = ForeachUnion(omr, occurmids, 
        Singleton(Tuple("donorId" -> omr("donorId"), "transcript_consequences" -> 
        ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
          Singleton(Tuple("gene_id" -> amr("gene_id"), "score0" -> matchImpactMid * siftImpact * polyImpact,
            "consequence_terms" -> 
          ForeachUnion(cr, BagProject(amr, "consequence_terms"),
            ForeachUnion(conr, conseq, 
              IfThenElse(Cmp(OpEq, conr("so_term"), cr("element")),
                  Singleton(Tuple("score1" -> conr("so_weight").asNumeric)))))))))))
  
  val (step0, s0r) = varset("step0", "s0", step0Query)
  val s0r2 = TupleVarRef("s02", BagProject(s0r, "transcript_consequences").tp.tp)
  val s0r3 = TupleVarRef("s03", BagProject(s0r2, "consequence_terms").tp.tp)
  
  val query = ForeachUnion(br, biospec,
            Singleton(Tuple("hybrid_sample" -> br("bcr_patient_uuid"), 
              "hybrid_aliquot" -> br("bcr_aliquot_uuid"),
              "hybrid_center" -> br("center_id"),
              "hybrid_genes" -> ReduceByKey(
                ForeachUnion(s0r, step0, 
                  IfThenElse(Cmp(OpEq, s0r("donorId"), br("bcr_patient_uuid")),
                    ForeachUnion(s0r2, BagProject(s0r, "transcript_consequences"),
                      ForeachUnion(cncr, cnvCases,
                        IfThenElse(And(Cmp(OpEq, cncr("cn_case_uuid"), s0r("donorId")),
                        Cmp(OpEq, s0r2("gene_id"), cncr("cn_gene_id"))),
                        ForeachUnion(s0r3, BagProject(s0r2, "consequence_terms"),
                          Singleton(Tuple("hybrid_gene_id" -> s0r2("gene_id"),
                            "hybrid_score" -> s0r3("score1").asNumeric * s0r2("score0").asNumeric *
                            (cncr("cn_copy_number").asNumeric + NumericConst(.01, DoubleType)))))))))),
                List("hybrid_gene_id"),
                List("hybrid_score")))))

  val program = Program(Assignment("cnvCases", mapCNV), Assignment("step0", step0Query), Assignment(name, query))

}

object SampleNetworkNew extends DriverGene {

  val name = "SampleNetworkNew"

  // brca specific
  val sampleFile = "/nfs_qc4/genomics/gdc/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_brca.txt"
  val cnvFile = "/nfs_qc4/genomics/gdc/gene_level/brca/"
  val occurFile = "/nfs_qc4/genomics/gdc/somatic/"
  val occurName = "datasetBrca"
  val occurDicts = ("odictBrca1", "odictBrca2", "odictBrca3")

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${loadBiospec(shred, skew, fname = sampleFile)}
        |${loadConseqs(shred, skew)}
        |${loadOccurrence(shred, skew, fname = occurFile, iname = occurName, dictNames = occurDicts)}
        |${loadCopyNumber(shred, skew, fname = cnvFile)}
        |${loadNetwork(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |""".stripMargin

  val (hybrid, hmr) = varset(HybridBySampleNew.name, "hm", HybridBySampleNew.program(HybridBySampleNew.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val fnet = ForeachUnion(nr, network, 
      ForeachUnion(er, BagProject(nr, "edges"),
        ForeachUnion(gpr, gpmap,
         IfThenElse(Cmp(OpEq, er("edge_protein"), gpr("protein_stable_id")),
          Singleton(Tuple("network_node" -> nr("node_protein"), 
            "network_edge" -> gpr("gene_stable_id"),
            "network_combined" -> er("combined_score")))))))

  val (fNet, mfr) = varset("flatNet", "fn", fnet)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_aliquot" -> hmr("hybrid_aliquot"),
        "network_center" -> hmr("hybrid_center"), 
          "network_genes" ->  ReduceByKey(
                ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
                  ForeachUnion(mfr, fNet, 
                    IfThenElse(Cmp(OpEq, mfr("network_edge"), gene("hybrid_gene_id")),
                      Singleton(Tuple("network_protein_id" -> mfr("network_node"),
                        "distance" -> mfr("network_combined").asNumeric * gene("hybrid_score").asNumeric
                        ))))),
              List("network_protein_id"),
              List("distance")))))


  val program = HybridBySampleNew.program.asInstanceOf[SampleNetworkNew.Program]
    .append(Assignment(fNet.name, fnet)).append(Assignment(name, query))

}

object EffectBySampleNew extends DriverGene {

  val name = "EffectBySampleNew"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadNetwork(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |""".stripMargin

  val (hybrid, hmr) = varset(HybridBySampleNew.name, "hm", HybridBySampleNew.program(HybridBySampleNew.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)
  val (snetwork, snr) = varset(SampleNetworkNew.name, "sn",SampleNetworkNew.program(SampleNetworkNew.name).varRef.asInstanceOf[BagExpr])
  val gene2 = TupleVarRef("ngene", snr.tp("network_genes").asInstanceOf[BagType].tp)

  val flatSampleNetwork = ForeachUnion(snr, snetwork,
    ForeachUnion(gene2, BagProject(snr, "network_genes"),
      ForeachUnion(gpr, gpmap,
        IfThenElse(Cmp(OpEq, gene2("network_protein_id"), gpr("protein_stable_id")),
          Singleton(Tuple("fnetwork_aliquot" -> snr("network_aliquot"),
            "fnetwork_protein_id" -> gene2("network_protein_id"),
            "fnetwork_gene_id" -> gpr("gene_stable_id"),
            "fnetwork_distance" -> gene2("distance")))))))

  val (flatNet, fner) = varset("flatSampNet", "fnp", flatSampleNetwork)

  val query = ForeachUnion(hmr, hybrid, 
          Singleton(Tuple("effect_sample" -> hmr("hybrid_sample"), 
            "effect_aliquot" -> hmr("hybrid_aliquot"),
            "effect_center" -> hmr("hybrid_center"),
            "effect_genes" -> 
            ForeachUnion(gene1, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(fner, flatNet,
                IfThenElse(And(Cmp(OpEq, hmr("hybrid_aliquot"), fner("fnetwork_aliquot")),  
                  Cmp(OpEq, gene1("hybrid_gene_id"), fner("fnetwork_gene_id"))),
                    Singleton(Tuple("effect_gene_id" -> gene1("hybrid_gene_id"), 
                      "effect_protein_id" -> fner("fnetwork_protein_id"),
                      "effect" -> gene1("hybrid_score").asNumeric * fner("fnetwork_distance").asNumeric))))))))

  val program = SampleNetworkNew.program.asInstanceOf[EffectBySampleNew.Program]
    .append(Assignment(flatNet.name, flatSampleNetwork))
    .append(Assignment(name, query))

}

object ConnectionBySampleNew extends DriverGene {

  val name = "ConnectionBySampleNew"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadNetwork(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadGeneExpr(shred, skew)}
        |""".stripMargin

  val (effect, emr) = varset(EffectBySampleNew.name, "em", EffectBySampleNew.program(EffectBySampleNew.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("egene", emr.tp("effect_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(emr, effect, 
        Singleton(Tuple("connection_sample" -> emr("effect_sample"), 
          "connection_aliquot" -> emr("effect_aliquot"),
          "connection_center" -> emr("effect_center"),
          "connect_genes" -> ForeachUnion(gene1, BagProject(emr, "effect_genes"),
            ForeachUnion(fexpr, fexpression,
              IfThenElse(And(Cmp(OpEq, gene1("effect_gene_id"), fexpr("ge_gene_id")),
                Cmp(OpEq, emr("effect_aliquot"), fexpr("ge_aliquot"))),
                Singleton(Tuple("connect_gene" -> gene1("effect_gene_id"), 
                  "connect_protein" -> gene1("effect_protein_id"),
                  "gene_connectivity" -> gene1("effect").asNumeric * fexpr("ge_fpkm").asNumeric))))))))

  val program = EffectBySampleNew.program.asInstanceOf[ConnectionBySampleNew.Program].append(Assignment(name, query))

}

object GeneConnectivityNew extends DriverGene {

  val name = "GeneConnectivityNew"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadNetwork(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadGeneExpr(shred, skew)}
        |""".stripMargin

  val (connect, cmr) = varset(ConnectionBySampleNew.name, "em", ConnectionBySampleNew.program(ConnectionBySampleNew.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("cgene", cmr.tp("connect_genes").asInstanceOf[BagType].tp)

  val query = ReduceByKey(ForeachUnion(cmr, connect, 
      ForeachUnion(gene1, BagProject(cmr, "connect_genes"),
        Singleton(Tuple("gene_id" -> gene1("connect_gene"), 
          "connectivity" -> gene1("gene_connectivity"))))),
        List("gene_id"),
        List("connectivity"))

  val program = ConnectionBySampleNew.program.asInstanceOf[GeneConnectivityNew.Program].append(Assignment(name, query))

}
