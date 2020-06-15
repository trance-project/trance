package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.MaterializeNRC

trait HiC extends MaterializeNRC {

  def loadProcessedHic(path: List[(String, String)], shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) "//TODO"
    else {
      val inputs = path.map(p => s"""|val ${p._1} = hicLoader.load("${p._2}")""").mkString("\n")
      s"""|val hicLoader = new HiCLoader(spark)
          $inputs
          |""".stripMargin
    }
  }

  val hic_oType = List(("chr", StringType), ("tss_bin_start", IntType), 
    ("tss_bin_end", IntType), ("interacting_bin_start", IntType), 
    ("interacting_bin_end", IntType), ("fdr", StringType), 
    ("gene_id", StringType))

  val hicType = TupleType(hic_oType.toMap)

  val cpEnhancer = BagVarRef("cpEnhancers", BagType(hicType))
  val cpr = TupleVarRef("cp", hicType)

  val gzEnhancer = BagVarRef("gzEnhancers", BagType(hicType))
  val gzr = TupleVarRef("gz", hicType)

}

trait CapHiC extends MaterializeNRC {

  def loadProcessedCapHic(path: List[(String, String)], shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) "//TODO"
    else {
      val inputs = path.map(p => s"""|val ${p._1} = capHicLoader.load("${p._2}")""").mkString("\n")
      s"""|val capHicLoader = new CapHiCLoader(spark)
          $inputs
          |""".stripMargin
    }
  }

  val cap_oType = List(("gene_id", StringType), ("official_name", StringType), 
    ("cap4_enhancer_no", IntType))

  val capType = TupleType(cap_oType.toMap)

  val capEnhancers = BagVarRef("capEnhancers", BagType(capType))
  val capr = TupleVarRef("cap", capType)

}

trait Fantom extends MaterializeNRC {

  def loadProcessedFantom(path: String, shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) "//TODO"
    else 
      s"""|val fantomLoader = new FantomLoader(spark)
          |val fantom = fantomLoader.load("$path")
          |""".stripMargin
  }

  // this should only join with gene when fdr_stat < 1
  // join with gene on gene_name
  val fantom_oType = List(("chrom",StringType), ("chrom_start", IntType), 
    ("chrom_end", IntType),("chr",StringType),("pos_range", StringType), 
    ("ncbi_id",StringType), ("gene_name",StringType), ("r",StringType),
    ("r_stat",DoubleType), ("fdr",StringType), ("fdr_stat", IntType),
    ("score", IntType), ("strand", StringType), ("thick_start",IntType), 
    ("thick_end", IntType), ("item_rgb", StringType), 
    ("block_count", IntType), ("block_sizes", StringType), 
    ("chrom_starts", StringType))
  val fantomType = TupleType(fantom_oType.toMap)

  val fantomEnhancers = BagVarRef("fantom", BagType(fantomType))
  val fanr = TupleVarRef("fan", fantomType)

}

trait Enhancers extends Query with GeneLoader with Fantom with CapHiC with HiC {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    val basepath = "/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files"
    val hics = List(("cpEnhancers", s"$basepath/BrainHiC/S22_TSS_CP.txt"), 
      ("gzEnhancers", s"$basepath/BrainHiC/S23_TSS_GZ.txt"))
    val caps = List(("capEnhancers", s"$basepath/capHiC/GM12878_DRE_number"))
    val fs = s"$basepath/Fantom5/enhancer_tss_associations.csv"
    s"""|${loadProcessedHic(hics)}
        |${loadProcessedCapHic(caps)}
        |${loadProcessedFantom(fs)}
        |${loadGene(s"$basepath/All_human_genes", shred, skew)}
        |""".stripMargin
  }

}

object GeneEnhancers extends Enhancers {

  val name = "GeneEnhancers"

  val query = ForeachUnion(gene, genes, 
      Singleton(Tuple("gene_id" -> gene("name"), "gene_name" -> gene("alias_symbol"),
        "cpEnhancers" -> ForeachUnion(cpr, cpEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), cpr("gene_id")), Singleton(cpr))),
        "gzEnhancers" -> ForeachUnion(gzr, gzEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), gzr("gene_id")), Singleton(gzr))),
        "capEnhancers" -> ForeachUnion(capr, capEnhancers, 
          IfThenElse(Cmp(OpEq, gene("name"), capr("gene_id")), Singleton(capr))),
        "fantomEnhancers" -> ForeachUnion(fanr, fantomEnhancers, 
          IfThenElse(Cmp(OpEq, gene("alias_symbol"), fanr("gene_name")), Singleton(fanr)))
        )))

  val program = Program(Assignment(name, query))

}

object GeneEnhancersMin extends Enhancers {

  val name = "GeneEnhancersMin"

  val query = ForeachUnion(gene, genes, 
      Singleton(Tuple("gene_id" -> gene("name"), "gene_name" -> gene("alias_symbol"),
        "cpEnhancers" -> Count(ForeachUnion(cpr, cpEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), cpr("gene_id")), 
            Singleton(Tuple("gene_id" -> cpr("gene_id")))))),
        "gzEnhancers" -> Count(ForeachUnion(gzr, gzEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), gzr("gene_id")), Singleton(Tuple("gene_id" -> gzr("gene_id")))))),
        "capEnhancers" -> Count(ForeachUnion(capr, capEnhancers, 
          IfThenElse(Cmp(OpEq, gene("name"), capr("gene_id")), Singleton(Tuple("gene_id" -> capr("gene_id")))))),
        "fantomEnhancers" -> Count(ForeachUnion(fanr, fantomEnhancers, 
          IfThenElse(Cmp(OpEq, gene("alias_symbol"), fanr("gene_name")), Singleton(Tuple("gene_name" -> fanr("gene_name"))))))
        )))

  val program = Program(Assignment(name, query))

}


object GeneEnhancersReduce extends Enhancers {

  val name = "GeneEnhancersReduce"

  val query = ForeachUnion(gene, genes, 
      Singleton(Tuple("gene_id" -> gene("name"), "gene_name" -> gene("alias_symbol"),
        "cpEnhancers" -> ReduceByKey(ForeachUnion(cpr, cpEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), cpr("gene_id")), 
            Singleton(Tuple("gene_id" -> cpr("gene_id"), "cnt" -> Const(1, IntType))))), List("gene_id"), List("cnt")),
        "gzEnhancers" -> ReduceByKey(ForeachUnion(gzr, gzEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), gzr("gene_id")), 
            Singleton(Tuple("gene_id" -> gzr("gene_id"), "cnt" -> Const(1, IntType))))), List("gene_id"), List("cnt")),
        "capEnhancers" -> ReduceByKey(ForeachUnion(capr, capEnhancers, 
          IfThenElse(Cmp(OpEq, gene("name"), capr("gene_id")), 
            Singleton(Tuple("gene_id" -> capr("gene_id"), "cnt" -> Const(1, IntType))))), List("gene_id"), List("cnt")),
        "fantomEnhancers" -> ReduceByKey(ForeachUnion(fanr, fantomEnhancers, 
          IfThenElse(Cmp(OpEq, gene("alias_symbol"), fanr("gene_name")), 
            Singleton(Tuple("gene_name" -> fanr("gene_name"), "cnt" -> Const(1, IntType))))), List("gene_name"), List("cnt"))
        )))

  val program = Program(Assignment(name, query))

}

/** ReduceByKey test, when key is empty this should behave similar to count
  *
  */
object GeneEnhancersReduce2 extends Enhancers {

  val name = "GeneEnhancersReduce2"

  val query = ForeachUnion(gene, genes, 
      Singleton(Tuple("gene_id" -> gene("name"), "gene_name" -> gene("alias_symbol"),
        "cpEnhancers" -> ReduceByKey(ForeachUnion(cpr, cpEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), cpr("gene_id")), 
            Singleton(Tuple("gene_id" -> cpr("gene_id"))))), Nil, List("gene_id")),
        "gzEnhancers" -> ReduceByKey(ForeachUnion(gzr, gzEnhancer, 
          IfThenElse(Cmp(OpEq, gene("name"), gzr("gene_id")), 
            Singleton(Tuple("gene_id" -> gzr("gene_id"))))), Nil, List("gene_id")),
        "capEnhancers" -> ReduceByKey(ForeachUnion(capr, capEnhancers, 
          IfThenElse(Cmp(OpEq, gene("name"), capr("gene_id")), 
            Singleton(Tuple("gene_id" -> capr("gene_id"))))), Nil, List("gene_id")),
        "fantomEnhancers" -> ReduceByKey(ForeachUnion(fanr, fantomEnhancers, 
          IfThenElse(Cmp(OpEq, gene("alias_symbol"), fanr("gene_name")), 
            Singleton(Tuple("gene_name" -> fanr("gene_name"))))), Nil, List("gene_name"))
        )))

  val program = Program(Assignment(name, query))

}



