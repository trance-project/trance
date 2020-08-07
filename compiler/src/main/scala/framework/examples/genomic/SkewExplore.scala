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
        IfThenElse(Cmp(OpEq, cnr("cn_gene_id"), fnr2("network_node")), 
          projectTuple(cnr, Map("edge" -> fnr2("network_edge"), 
            "score" -> fnr2("network_combined").asNumeric * cnr("cn_copy_number").asNumeric))))),
      List("cn_gene_id"),
      List("score"))

  val program = Program(Assignment("flatNet", fnet), Assignment(name, query))

}