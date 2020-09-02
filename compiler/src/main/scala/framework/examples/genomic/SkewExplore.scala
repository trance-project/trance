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


object SkewTest6 extends DriverGene {

  val name = "SkewTest6"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGtfTable(shred, skew)}""".stripMargin

  val query = 
      ReduceByKey(
        ForeachUnion(omr, occurmids,
          ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
            ForeachUnion(gtfr, gtf,
              IfThenElse(Cmp(OpEq, amr("gene_id"), gtfr("g_gene_id")), 
                Singleton(Tuple("case_uuid" -> amr("donorId"), 
                  "c_gene_id" -> gtfr("c_gene_id"),
                  "name" -> gtfr("g_gene_name"),
                  "count" -> NumericConst(1.0, DoubleType))))))),
              List("case_uuid", "c_gene_id", "name"),
              List("count"))


  val program = Program(Assignment(name, query))

}

object SkewTest7 extends DriverGene {

  val name = "SkewTest7"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadFlatNetwork(shred, skew)}""".stripMargin

  val query = 
   ReduceByKey( 
    ForeachUnion(omr, occurmids,
      ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
        ForeachUnion(gpr, gpmap,
          IfThenElse(Cmp(OpEq, amr("gene_id"), gpr("gene_stable_id")),
            ForeachUnion(fnr, fnetwork,
              IfThenElse(Cmp(OpEq, fnr("protein1"), gpr("protein_stable_id")),
                Singleton(Tuple("case_uuid" -> omr("donorId"),
                                "gene" -> amr("gene_id"), 
                                "protein" -> fnr("protein1"), 
                                "score" -> fnr("combined_score").asNumeric * matchImpactMid)))))))),
                  List("case_uuid", "gene", "ngene"),
                  List("score"))

  val program = Program(Assignment(name, query))

}

object SkewTest8 extends DriverGene {

  val name = "SkewTest8"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadFlatNetwork(shred, skew)}""".stripMargin

  val fnet = ForeachUnion(fnr, fnetwork, 
        ForeachUnion(gpr, gpmap,
         IfThenElse(Cmp(OpEq, fnr("protein1"), gpr("protein_stable_id")),
          Singleton(Tuple("network_node" -> fnr("protein1"), 
            "network_gene" -> gpr("gene_stable_id"),
            "network_combined" -> fnr("combined_score"))))))
  
  val (flatNet, fnr2) = varset("flatNet", "fn2", fnet)

  val query = 
    ForeachUnion(br, biospec,
      Singleton(Tuple("sample" -> br("bcr_patient_uuid"), "genes" -> 
        ReduceByKey(ForeachUnion(omr, occurmids,
          IfThenElse(Cmp(OpEq, br("bcr_patient_uuid"), omr("donorId")),
            ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
              ForeachUnion(fnr2, flatNet,
                IfThenElse(Cmp(OpEq, amr("gene_id"), fnr2("network_gene")),
                  Singleton(Tuple("gene" -> amr("gene_id"), 
                          "protein" -> fnr2("network_node"), 
                          "score" -> fnr2("network_combined").asNumeric * matchImpactMid))))))),
                  List("gene", "protein"),
                  List("score")))))

  val program = Program(Assignment("flatNet", fnet), Assignment(name, query))

}

object SkewTest9 extends DriverGene {

  val name = "SkewTest9"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadGeneProteinMap(shred, skew)}
        |${loadCopyNumber(shred, skew)}
        |${loadNetwork(shred, skew)}""".stripMargin

  val query = ForeachUnion(nr, network, 
    Singleton(Tuple("n_node" -> nr("node_protein"), "n_edges" -> 
      ReduceByKey(ForeachUnion(er, BagProject(nr, "edges"),
        ForeachUnion(gpr, gpmap,
          IfThenElse(Cmp(OpEq, er("edge_protein"), gpr("protein_stable_id")),
            ForeachUnion(cnr, copynum,
              IfThenElse(Cmp(OpEq, cnr("cn_gene_id"), gpr("gene_stable_id")),
            Singleton(Tuple("n_edge" -> er("edge_protein"),
              "n_gene" -> gpr("gene_stable_id"),
              "sample" -> cnr("cn_aliquot_uuid"),
              "n_score" -> (er("combined_score").asNumeric * 
                cnr("cn_copy_number").asNumeric + NumericConst(0.0, DoubleType))
              ))))))), 
        List("n_edge", "n_gene", "sample"), 
        List("n_score")))))

  val program = Program(Assignment(name, query))

}




// object SkewTest7 extends DriverGene {

//   val name = "SkewTest7"

//   override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
//     s"""|${super.loadTables(shred, skew)}
//         |${loadGtfTable(shred, skew)}""".stripMargin

//   val query = 
//       ReduceByKey(
//         ForeachUnion(omr, occurmids,
//           ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
//             ForeachUnion(gtfr, gtf,
//               IfThenElse(Cmp(OpEq, amr("gene_id"), gtfr("g_gene_id")), 
//                 Singleton(Tuple("case_uuid" -> amr("donorId"), 
//                   "c_gene_id" -> gtfr("c_gene_id")
//                   "name" -> gtfr("g_gene_name"),
//                   "distance" -> 
//                     IfThenElse(OpLt(omr("vstart").asNumeric, gtfr()

//                     Const(1.0, DoubleType))))))),
//               List("case_uuid", "c_gene_id", "name"),
//               List("count"))


//   val program = Program(Assignment(name, query))

// }

// object SkewTest6 extends DriverGene {

//   val name = "SkewTest6"

//   override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
//     s"""|${super.loadTables(shred, skew)}
//         |${loadGtfTable(shred, skew)}""".stripMargin

//   val query = 
//   // ForeachUnion(gpr, gpmap,
//   //   Singleton(Tuple("group_gene" -> grp("gene_stable_id"), "group_protein" -> grp("group_protein_id"),
//   //     "grouped_mutations" -> 
//         ForeachUnion(omr, occurmids,
//           Singleton(Tuple("case_uuid" -> omr("donorId"), "cands" -> 
//             ReduceByKey(ForeachUnion(amr, BagProject(omr, "transcript_consequences"),
//               ForeachUnion(gtfr, gtf,
//                 IfThenElse(Cmp(OpEq, amr("gene_id"), gtfr("g_gene_id")), 
//                   Singleton(Tuple("gene" -> amr("gene_id"), 
//                     "name" -> gtfr("g_gene_name"),
//                     "cnt" -> Const(0.0, DoubleType))))))))))
//             // List("gene", "name"),
//             // List("cnt"))


//   val program = Program(Assignment(name, query))

// }
