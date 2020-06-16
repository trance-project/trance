package framework.examples.genomic

import framework.common._
import framework.examples.Query

/** This document contains a series of queries for performing 
  * an odds ratio calculation in NRC, including variants on this query. 
  */

trait GenomicBase extends Query with VCF with TGenomesMetadata with GeneLoader {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    s"""|${loadVCF("/Users/jac/bioqueries/data/sub.vcf", shred, skew)}
        |${loadTGenomes("/Users/jac/bioqueries/data/1000g.csv", shred, skew)}
        |${loadGene("/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes", shred, skew)}
        |""".stripMargin
  }

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
  val (snps, sr) = varset(OddsRatio.name, "snp", OddsRatio.program(OddsRatio.name).varRef.asInstanceOf[BagExpr])

  val query = ForeachUnion(sr, snps,
    IfThenElse(Cmp(OpGt, sr("odds"), Const(1, IntType)), 
      Singleton(Tuple("contig" -> sr("contig"), "start" -> sr("start"), 
        "reference" -> sr("reference"), "alternate" -> sr("alternate"), 
        "candidateGenes" -> ForeachUnion(gene, genes,
          IfThenElse(And(Cmp(OpEq, sr("contig"), gene("chrom")),And(Cmp(OpGe, sr("start"), gene("start_hg19")),
            Cmp(OpGe, gene("end_hg19"), sr("start")))),
          Singleton(Tuple("id" -> gene("name"), "name" -> gene("alias_symbol"), "strand" -> gene("strand")))))))))

  val program = OddsRatio.program.asInstanceOf[MappedGenes.Program].append(Assignment(name, query))

}

/** Associate SNPs to flanking region **/

object CandidateGenes extends GenomicBase {
  
  val name = "CandidateGenes"
  val (snps, sr) = varset(OddsRatio.name, "snp", OddsRatio.program(OddsRatio.name).varRef.asInstanceOf[BagExpr])

  val flank = Const(1000000, IntType).asNumeric
  val query = ForeachUnion(sr, snps,
    IfThenElse(Cmp(OpGt, sr("odds"), Const(1, IntType)), 
      Singleton(Tuple("contig" -> sr("contig"), "start" -> sr("start"), 
        "reference" -> sr("reference"), "alternate" -> sr("alternate"), 
        "candidateGenes" -> ForeachUnion(gene, genes,
          IfThenElse(And(Cmp(OpEq, sr("contig"), gene("chrom")),
            And(Cmp(OpGe, sr("start"), gene("start_hg19").asNumeric - flank),
              Cmp(OpGe, gene("end_hg19").asNumeric + flank, sr("start")))),
          Singleton(Tuple("gene_id" -> gene("name"), "gene_name" -> gene("alias_symbol"), "strand" -> gene("strand")))))))))

  val program = OddsRatio.program.asInstanceOf[CandidateGenes.Program].append(Assignment(name, query))

}

/** Alternate version of the pipeline that flattens at the start **/

object ORStep1Alt extends GenomicBase {

  val name = "ORStep1"

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
