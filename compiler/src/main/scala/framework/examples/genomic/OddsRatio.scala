package framework.examples.genomic

import framework.common._
import framework.examples.Query

/** This document contains a series of queries for performing 
  * an odds ratio calculation in NRC, including variants on this query. 
  */
trait GenomicBase extends Query {
  
  def inputTypes(shred: Boolean = false): Map[Type, String] = Map()
  
  def headerTypes(shred: Boolean = false): List[String] = Nil

  override def loadTables(tbls: Set[String], eval: String, shred: Boolean = false, skew: Boolean = false): String = {
    if (shred)
      s"""|val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
          |val (variants, genotypes) = vloader.shredDS
          |val IBag_Variants__D = variants
          |val IDict_variants__D_genotypes = genotypes
          |val cloader = new ClinicalLoader(spark, "/Users/jac/bioqueries/data/1000g.csv")
          |val IBag_Metadata__D = cloader.table
          |val gloader = new GeneLoader(spark, "/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes")
          |val IBag_Gene__D = gloader.table
          |""".stripMargin
    else if (skew)
      s"""|val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
          |val Variants_L = vloader.loadDS
          |val Variants = (Variants_L, Variants_L.empty)
          |val cloader = new ClinicalLoader(spark, "/Users/jac/bioqueries/data/1000g.csv")
          |val Metadata_L = cloader.table
          |val Metadata = (Metadata_L, Metadata_L.empty)
          |val gloader = new GeneLoader(spark, "/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes")
          |val Gene_L = gloader.table
          |val Gene = (Gene_L, Gene_L.empty)
          |""".stripMargin
    else 
      s"""|val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
          |val Variants = vloader.loadDS
          |val cloader = new ClinicalLoader(spark, "/Users/jac/bioqueries/data/1000g.csv")
          |val Metadata = cloader.table
          |val gloader = new GeneLoader(spark, "/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes")
          |val Gene = gloader.table
          |""".stripMargin
  }

  /** Odds Ratio Types **/

  val genoType = TupleType("g_sample" -> StringType, "call" -> IntType)
  val variantType = TupleType("contig" -> StringType, "start" -> IntType, 
    "reference" -> StringType, "alternate" -> StringType, "genotypes" -> BagType(genoType))
  
  val variants = BagVarRef("Variants", BagType(variantType))
  val vr = TupleVarRef("variant", variantType)
  val gr = TupleVarRef("genotype", genoType)

  val metaType = TupleType("m_sample" -> StringType, "family_id" -> StringType, 
    "population" -> StringType, "gender" -> StringType)
  val metadata = BagVarRef("Metadata", BagType(metaType))
  val mr = TupleVarRef("metadata", metaType)

  val geneType = TupleType("name" -> StringType,
    "description" -> StringType,
    "chrom" -> StringType,
    "g_type" -> StringType,
    "start_hg19" -> IntType,
    "end_hg19" -> IntType,
    "strand" -> StringType,
    "ts_id" -> StringType,
    "gene_type" -> StringType,
    "gene_status" -> StringType,
    "loci_level" -> IntType,
    "alias_symbol" -> StringType,
    "official_name" -> StringType)
  val genes = BagVarRef("Gene", BagType(geneType))
  val g2r = TupleVarRef("gene", geneType)

  /** GWAS types -- to merge **/
  val relI = BagVarRef("cases", BagType(GenomicRelations.casetype))
  val iref = TupleVarRef("i", GenomicRelations.casetype)

  val relV = BagVarRef("variants", BagType(GenomicRelations.varianttype))
  val vref = TupleVarRef("v", GenomicRelations.varianttype)
  val gref = TupleVarRef("g", GenomicRelations.genotype)

  val relC = BagVarRef("clinical", BagType(GenomicRelations.clintype))
  val cref = TupleVarRef("c", GenomicRelations.clintype)

}

/** The first step in the odds ratio calculation: 
  * get the reference and allele count for a binary variable.
  * In a clinical case this woudl be is-case or is-control, but
  * using gender for the 1000 genomes case for example purposes.
  * 
  * An alternate version of this will perform the reduce by key at the top.
  * (see ORStep1Alt)
  */
object ORStep1 extends GenomicBase {

  val name = "ORStep1"
  def inputs(tmap: Map[String, String]): String = ""

  val heterozyg = Cmp(OpEq, gr("call"), Const(1, IntType))
  val homozyg = Cmp(OpEq, gr("call"), Const(2, IntType))
  val query = 
    ForeachUnion(vr, variants, 
      Singleton(Tuple("contig" -> vr("contig"), 
        "start" -> vr("start"),
        "reference" -> vr("reference"),
        "alternate" -> vr("alternate"),
        "calls" ->     
        ReduceByKey(
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            ForeachUnion(mr, metadata,
              IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
                Singleton(Tuple("gender" -> mr("gender"), "refCnt" -> 
                  PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                    PrimitiveIfThenElse(homozyg, Const(0.0, DoubleType), Const(2.0, DoubleType))),
                  "altCnt" -> PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                    PrimitiveIfThenElse(homozyg, Const(2.0, DoubleType), Const(0.0, DoubleType)))
                  ))))),
          List("gender"),
          List("refCnt", "altCnt")))))

  val program = Program(Assignment(name, query))
}

object ORStep2 extends GenomicBase {

  val name = "ORStep2"
  def inputs(tmap: Map[String, String]): String = ""

  val (cnts, ac) = varset(ORStep1.name, "v2", ORStep1.program(ORStep1.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("calls").asInstanceOf[BagExpr].tp.tp)
  val query = 
    ReduceByKey(
      ForeachUnion(ac, cnts, 
        ForeachUnion(ac2, BagProject(ac, "calls"),
          Singleton(Tuple(
            "contig" -> ac("contig"), 
            "start" -> ac("start"),
            "reference" -> ac("reference"),
            "alternate" -> ac("alternate"),
            "maleRatio" -> 
              PrimitiveIfThenElse(Cmp(OpEq, ac2("gender"), Const("male", StringType)),
              ac2("altCnt").asNumeric / ac2("refCnt").asNumeric, Const(0.0, DoubleType)),
            "femaleRatio" ->  
              PrimitiveIfThenElse(Cmp(OpEq, ac2("gender"), Const("female", StringType)),
              ac2("altCnt").asNumeric / ac2("refCnt").asNumeric, Const(0.0, DoubleType))
            )))),
      List("contig", "start", "reference", "alternate"),
      List("maleRatio", "femaleRatio"))

  val program = ORStep1.program.asInstanceOf[ORStep2.Program].append(Assignment(name, query))
}

/** The final step in the odds ratio calculation, take 
  * the ratio of ratios. 
  * TODO: maybe a log-scale?
  */
object OddsRatio extends GenomicBase {

  val name = "OddsRatio"
  def inputs(tmap: Map[String, String]): String = ""

  val (cnts, ac) = varset(ORStep2.name, "v3", ORStep2.program(ORStep2.name).varRef.asInstanceOf[BagExpr])
  val query = 
    ForeachUnion(ac, cnts, 
      Singleton(Tuple(
        "contig" -> ac("contig"), 
        "start" -> ac("start"),
        "reference" -> ac("reference"),
        "alternate" -> ac("alternate"),
        "odds" -> ac("femaleRatio").asNumeric / ac("maleRatio").asNumeric)))

  val program = ORStep2.program.asInstanceOf[OddsRatio.Program].append(Assignment(name, query))
}

/** Associate SNPs to Genes **/

object MappedGenes extends GenomicBase {
  
  val name = "MappedGenes"
  def inputs(tmap: Map[String, String]): String = ""
  val (snps, sr) = varset(OddsRatio.name, "snp", OddsRatio.program(OddsRatio.name).varRef.asInstanceOf[BagExpr])

  val query = ForeachUnion(sr, snps,
    IfThenElse(Cmp(OpGt, sr("odds"), Const(1, IntType)), 
      Singleton(Tuple("contig" -> sr("contig"), "start" -> sr("start"), 
        "reference" -> sr("reference"), "alternate" -> sr("alternate"), 
        "candidateGenes" -> ForeachUnion(g2r, genes,
          IfThenElse(And(Cmp(OpEq, sr("contig"), g2r("chrom")),And(Cmp(OpGe, sr("start"), g2r("start_hg19")),
            Cmp(OpGe, g2r("end_hg19"), sr("start")))),
          Singleton(Tuple("id" -> g2r("name"), "name" -> g2r("alias_symbol"), "strand" -> g2r("strand")))))))))

  val program = OddsRatio.program.asInstanceOf[MappedGenes.Program].append(Assignment(name, query))

}

/** Associate SNPs to flanking region **/

object CandidateGenes extends GenomicBase {
  
  val name = "CandidateGenes"
  def inputs(tmap: Map[String, String]): String = ""
  val (snps, sr) = varset(OddsRatio.name, "snp", OddsRatio.program(OddsRatio.name).varRef.asInstanceOf[BagExpr])

  val flank = Const(1000000, IntType).asNumeric
  val query = ForeachUnion(sr, snps,
    IfThenElse(Cmp(OpGt, sr("odds"), Const(1, IntType)), 
      Singleton(Tuple("contig" -> sr("contig"), "start" -> sr("start"), 
        "reference" -> sr("reference"), "alternate" -> sr("alternate"), 
        "candidateGenes" -> ForeachUnion(g2r, genes,
          IfThenElse(And(Cmp(OpEq, sr("contig"), g2r("chrom")),
            And(Cmp(OpGe, sr("start"), g2r("start_hg19").asNumeric - flank),
              Cmp(OpGe, g2r("end_hg19").asNumeric + flank, sr("start")))),
          Singleton(Tuple("id" -> g2r("name"), "name" -> g2r("alias_symbol"), "strand" -> g2r("strand")))))))))

  val program = OddsRatio.program.asInstanceOf[CandidateGenes.Program].append(Assignment(name, query))

}

/** Alternate version of the pipeline that flattens at the start **/

object ORStep1Alt extends GenomicBase {

  val name = "ORStep1"
  def inputs(tmap: Map[String, String]): String = ""

  val heterozyg = Cmp(OpEq, gr("call"), Const(1, IntType))
  val homozyg = Cmp(OpEq, gr("call"), Const(2, IntType))
  val query = 
    ReduceByKey(
      ForeachUnion(vr, variants, 
        ForeachUnion(gr, BagProject(vr, "genotypes"),
          ForeachUnion(mr, metadata,
            IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
              Singleton(Tuple("gender" -> mr("gender"), 
                "contig" -> vr("contig"), 
                "start" -> vr("start"),
                "reference" -> vr("reference"),
                "alternate" -> vr("alternate"), 
                "refCnt" -> 
                PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                  PrimitiveIfThenElse(homozyg, Const(0.0, DoubleType), Const(2.0, DoubleType))),
                    "altCnt" -> PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                    PrimitiveIfThenElse(homozyg, Const(2.0, DoubleType), Const(0.0, DoubleType)))
                  )))))),
          List("contig", "start", "reference", "alternate", "gender"),
          List("refCnt", "altCnt"))

  val program = Program(Assignment(name, query))
}


/** This was a first attempt at the allele counts for odds ratio calculuation. 
  * The query builds up an input for reduce with conditional attribute values.
  * 
  * Parsing this query was easier to do if the conditions were 
  * placed on the attributes themselves (rather than returning a whol tuple - see ORStep1)
  * 
  * Overall, a user could propose a query like this, but the conditions should be pushed to the 
  * relevant attributes, so that the appropriate columns can then be produced conditionally.
  * (ie. a potential normalization step)
  */
object OddsRatioInit extends GenomicBase {

  val name = "OddsRatioV2"
  def inputs(tmap: Map[String, String]): String = ""

  val query = 
    ForeachUnion(vr, variants, 
      Singleton(Tuple("contig" -> vr("contig"), 
        "start" -> vr("start"),
        "reference" -> vr("reference"),
        "alterante" -> vr("alternate"),
        "calls" ->     
        ReduceByKey(
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            ForeachUnion(mr, metadata,
              IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
                BagIfThenElse(Cmp(OpEq, gr("call"), Const(0, IntType)),
                  Singleton(Tuple("gender" -> mr("gender"), "refCnt" -> Const(2, IntType), "altCnt" -> Const(0, IntType))),
                Some(BagIfThenElse(Cmp(OpEq, gr("call"), Const(1, IntType)),
                  Singleton(Tuple("gender" -> mr("gender"), "refCnt" -> Const(1, IntType), "altCnt" -> Const(1, IntType))),
                Some(BagIfThenElse(Cmp(OpEq, gr("call"), Const(2, IntType)),
                  Singleton(Tuple("gender" -> mr("gender"), "refCnt" -> Const(0, IntType), "altCnt" -> Const(2, IntType))), None))))))
          )),
          List("gender"),
          List("refCnt", "altCnt")))))

  val program = Program(Assignment(name, query))
}
