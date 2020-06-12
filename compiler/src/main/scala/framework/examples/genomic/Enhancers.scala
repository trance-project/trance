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

  val hicType = TupleType("chr" -> StringType, "tss_bin_start" -> IntType, "tss_bin_end" -> IntType, 
    "interacting_bin_start" -> IntType, "interacting_bin_end" -> IntType, "fdr" -> StringType, 
    "gene_id" -> StringType)

  val cpEnhnacer = BagVarRef("cpEnhancers", BagType(hicType))
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

  val capType = TupleType("gene_id" -> StringType, "official_name" -> StringType, 
    "cap4_enhancer_no" -> IntType)

  val capEnhancers = BagVarRef("capEnhancers", BagType(capType))
  val capr = TupleVarRef("cap", capType)

}

trait Fantom extends MaterializeNRC {

  def loadProcessedFantom(path: String, shred: Boolean = false, skew: Boolean = false): String = {
    if (shred) "//TODO"
    else 
      s"""|val fantomLoader = new FantomLoader(spark)
          |val fantom = fantomLoader("$path")
          |""".stripMargin
  }

  // this should only join with gene when fdr_stat < 1
  // join with gene on gene_name
  val fantomType = TupleType("chrom" -> StringType, "chrom_start" -> IntType, "chrom_end" -> IntType,
    "chr" -> StringType,  "pos_range" -> StringType, "ncbi_id" -> StringType, "gene_name" -> StringType, "r" -> StringType,
    "r_stat" -> DoubleType, "fdr" -> StringType, "fdr_stat" -> IntType,
    "score" -> IntType, "strand" -> StringType, "thick_start" -> IntType, "thick_end" -> IntType, "item_rgb" -> StringType, 
      "block_count" -> IntType, "block_sizes" -> StringType, "chrom_starts" -> StringType)

  val fantomEnhancers = BagVarRef("fantom", BagType(fantomType))
  val fanr = TupleVarRef("fan", fantomType)

}

trait Enhancers extends Query with GeneLoader with Fantom with CapHiC with HiC {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    val basepath = "/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files"
    val hics = List(("cpEnhancers", s"$basepath/BrainHiC/S22_TSS_CP.txt"), 
      ("gzEnhancers", s"$basepath/BrainHiC/S23_TSS_GZ.txt"))
    val caps = List(("capEnhancers", s"$basepath/capHiC/GM12878_DRE_number"))
    val fs = s"$basepath/Fantom5/enhancer_tss_associations.bed.txt"
    s"""|${loadProcessedHic(hics)}
        |${loadProcessedCapHic(caps)}
        |${loadProcessedFantom(fs)}
        |""".stripMargin
  }

}