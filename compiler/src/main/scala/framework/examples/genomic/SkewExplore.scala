package framework.examples.genomic

import framework.common._
import framework.examples.Query

object SkewTest1 extends DriverGene {
  val name = "SkewTest1"
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"${super.loadTables(shred, skew)}\n${loadCopyNumber(shred, skew)}"

  val query = ForeachUnion(cnr, copynum,
      ForeachUnion(br, biospec,
        IfThenElse(Cmp(OpEq, cnr("cn_aliquot_uuid"), br("bcr_aliquot_uuid")), 
          projectTuple(cnr, Map("center_id" -> br("center_id"), "sample_id" -> br("bcr_patient_uuid"))))))

  val program = Program(Assignment(name, query))

}

object SkewTest2 extends DriverGene {

  val name = "SkewTest2"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadFlatNetwork(shred, skew)}""".stripMargin

  val fnet = ForeachUnion(fnr, fnetwork, 
        ForeachUnion(gpr, gpmap,
         IfThenElse(Cmp(OpEq, fnr("protein1"), gpr("protein_stable_id")),
          Singleton(Tuple("network_node" -> fnr("protein1"), 
            "network_node_gene" -> gpr("gene_stable_id"),
            "network_edge" -> fnr("protein2"),
            "network_combined" -> fnr("combined_score"))))))

  val (flatNet, fnr2) = varset("flatNet", "fn2", fnet)

  val query = ReduceByKey(ForeachUnion(cnr, copynum,
      ForeachUnion(fnr2, flatNet,
        IfThenElse(Cmp(OpEq, cnr("cn_gene_id"), fnr2("network_node_gene")), 
          projectTuple(cnr, Map("edge" -> fnr2("network_edge"), 
            "score" -> fnr2("network_combined").asNumeric * 
            (cnr("cn_copy_number").asNumeric + NumericConst(0.001, DoubleType))))))),
      List("cn_gene_id"),
      List("score"))

  val program = Program(Assignment("flatNet", fnet), Assignment(name, query))

}

object SkewTest3 extends DriverGene {

  val name = "SkewTest3"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadFlatNetwork(shred, skew)}""".stripMargin

  val fnet = ForeachUnion(fnr, fnetwork, 
        ForeachUnion(gpr, gpmap,
         IfThenElse(Cmp(OpEq, fnr("protein2"), gpr("protein_stable_id")),
          Singleton(Tuple("network_node" -> fnr("protein1"), 
            "network_edge_gene" -> gpr("gene_stable_id"),
            "network_edge" -> fnr("protein2"),
            "network_combined" -> fnr("combined_score"))))))
  
  val (flatNet, fnr2) = varset("flatNet", "fn2", fnet)

  val query = 
  // ForeachUnion(gpr, gpmap,
  //   Singleton(Tuple("group_gene" -> grp("gene_stable_id"), "group_protein" -> grp("group_protein_id"),
  //     "grouped_mutations" -> 
      ReduceByKey(
        ForeachUnion(omr, occurmids,
          ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
            ForeachUnion(fnr2, flatNet,
              IfThenElse(Cmp(OpEq, amr("gene_id"), fnr2("network_edge_gene")), 
                projectTuple(omr, Map("ngene" -> fnr2("network_node"), 
                  "nedge" -> fnr2("network_edge_gene"), "ndist" -> 
                  (fnr2("network_combined").asNumeric + NumericConst(0.0, DoubleType))), 
                List("transcript_consequences")))))),
        List("donorId", "ngene"),
        List("ndist"))


  val program = Program(Assignment("flatNet", fnet), Assignment(name, query))

}

object SkewTest4 extends DriverGene {

  val name = "SkewTest4"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadFlatNetwork(shred, skew)}""".stripMargin

  val fnet = ForeachUnion(fnr, fnetwork, 
        ForeachUnion(gpr, gpmap,
         IfThenElse(Cmp(OpEq, fnr("protein2"), gpr("protein_stable_id")),
          Singleton(Tuple("network_node" -> fnr("protein1"), 
            "network_edge_gene" -> gpr("gene_stable_id"),
            "network_edge" -> fnr("protein2"),
            "network_combined" -> fnr("combined_score"))))))
  
  val (flatNet, fnr2) = varset("flatNet", "fn2", fnet)

  val query = 
  // ForeachUnion(gpr, gpmap,
  //   Singleton(Tuple("group_gene" -> grp("gene_stable_id"), "group_protein" -> grp("group_protein_id"),
  //     "grouped_mutations" -> 
        ForeachUnion(omr, occurmids,
          Singleton(Tuple("case_uuid" -> omr("donorId"), "cands" -> 
            ReduceByKey(ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
              ForeachUnion(fnr2, flatNet,
                IfThenElse(Cmp(OpEq, amr("gene_id"), fnr2("network_edge_gene")), 
                  Singleton(Tuple("gene" -> amr("gene_id"), 
                    "ngene" -> fnr2("network_node"), 
                    "nedge" -> fnr2("network_edge_gene"), "ndist" -> 
                    (fnr2("network_combined").asNumeric + NumericConst(0.0, DoubleType))))))),
              List("ngene"),
              List("ndist")))))


  val program = Program(Assignment("flatNet", fnet), Assignment(name, query))

}

object SkewTest5 extends DriverGene {

  val name = "SkewTest5"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGtfTable(shred, skew)}""".stripMargin

  val query = 
  // ForeachUnion(gpr, gpmap,
  //   Singleton(Tuple("group_gene" -> grp("gene_stable_id"), "group_protein" -> grp("group_protein_id"),
  //     "grouped_mutations" -> 
        ForeachUnion(omr, occurmids,
          Singleton(Tuple("case_uuid" -> omr("donorId"), "cands" -> 
            ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
              ForeachUnion(gtfr, gtf,
                IfThenElse(Cmp(OpEq, amr("gene_id"), gtfr("g_gene_id")), 
                  Singleton(Tuple("gene" -> amr("gene_id"), 
                    "name" -> gtfr("g_gene_name")))))))))


  val program = Program(Assignment(name, query))

}
