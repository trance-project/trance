package shredding.examples.genomic

import shredding.core._
import shredding.nrc.ShredNRC

case class Clinical(sample: String, iscase: String)
case class Genotype(sample: String, call: Int)
case class Case(iscase: String)
case class Variant(contig: String, start: Int, genotypes: List[Genotype])
case class VariantFlat(contig: String, start: Int, genotypes: Int)

case class Genotype2(sample: String, call: Int, allele1: String, allele2: String)
case class Variant2(contig: String, start: Int, genotypes: List[Genotype2])


object GenomicRelations {

   val casetype = TupleType("iscase" -> StringType)
   val clintype = TupleType("sample" -> StringType, "iscase" -> StringType)
   val genotype = TupleType("sample" -> StringType, "call" -> IntType)
   val varianttype = TupleType("contig" -> StringType, "start" -> IntType, "consequence" -> StringType, "genotypes" -> BagType(genotype))
   val genotype2 = TupleType("sample" -> StringType, "call" -> IntType, "allele1" -> StringType, "allele2" -> StringType)
   val varianttype2 = TupleType("contig" -> StringType, "start" -> IntType, "genotypes" -> BagType(genotype2))


   val variantftype = TupleType("contig" -> StringType, "start" -> IntType, "genotypes" -> IntType)

   val q1inputs = Map[Type, String](casetype -> "Case", clintype -> "Clinical", 
                    genotype -> "Genotype", varianttype -> "Variant", variantftype -> "VariantFlat")

   val cases = List(Rec("iscase" -> "control"), Rec("iscase" -> "case"))
   val variants = List(
      Rec("contig" -> "1", "start" -> 100, "genotypes" -> 
        List(Rec("sample" -> "one", "call" -> 0), Rec("sample" -> "two", "call" -> 1), 
          Rec("sample" -> "three", "call" -> 2), Rec("sample" -> "four", "call" -> 0))),
      Rec("contig" -> "1", "start" -> 101, "genotypes" -> 
        List(Rec("sample" -> "one", "call" -> 1), Rec("sample" -> "two", "call" -> 1),
          Rec("sample" -> "three", "call" -> 0), Rec("sample" -> "four", "call" -> 1))),
      Rec("contig" -> "1", "start" -> 102, "genotypes" -> 
        List(Rec("sample" -> "one", "call" -> 2), Rec("sample" -> "two", "call" -> 0),
          Rec("sample" -> "three", "call" -> 1),Rec("sample" -> "four", "call" -> 2))))
      
   val clinical = List(
       Rec("sample" -> "one", "iscase" -> "case"), 
        Rec("sample" -> "two", "iscase" -> "case"), 
         Rec("sample" -> "three", "iscase" -> "control"), 
          Rec("sample" -> "four", "iscase" -> "control")) 
  
   val format1Spark = s"""
    |val cases = spark.sparkContext.parallelize(List(Case("control"), Case("case")))
    |val variants = spark.sparkContext.parallelize(List(
    |   Variant("1", 100, List(Genotype("one", 0), Genotype("two", 1), Genotype("three", 2), Genotype("four", 0))),
    |   Variant("1", 101, List(Genotype("one", 1), Genotype("two", 1), Genotype("three", 0), Genotype("four", 1))),
    |   Variant("1", 102, List(Genotype("one", 2), Genotype("two", 0), Genotype("three", 1), Genotype("four", 2)))))
    |val clinical = spark.sparkContext.parallelize(List(
    |    Clinical("one", "case"), Clinical("two", "case"), Clinical("three", "control"), Clinical("four", "control")))
    """.stripMargin 

   val cases__F = 1
   val cases__D = (List((cases__F, List(Rec("iscase" -> "control"), Rec("iscase" -> "case")))), ())

   val variants__D_2genotypes_1 = List(
    (3,  List(Rec("sample" -> "one", "call" -> 0), Rec("sample" -> "two", "call" -> 1),
          Rec("sample" -> "three", "call" -> 2), Rec("sample" -> "four", "call" -> 0))), 
    (4, List(Rec("sample" -> "one", "call" -> 1), Rec("sample" -> "two", "call" -> 1),
          Rec("sample" -> "three", "call" -> 0), Rec("sample" -> "four", "call" -> 1))), 
    (5, List(Rec("sample" -> "one", "call" -> 2), Rec("sample" -> "two", "call" -> 0),
          Rec("sample" -> "three", "call" -> 1),Rec("sample" -> "four", "call" -> 2))))

   val variants__F = 2
   val variants__D = (List((variants__F, List(
      Rec("contig" -> "1", "start" -> 100, "genotypes" -> 3), 
      Rec("contig" -> "1", "start" -> 101, "genotypes" -> 4),
      Rec("contig" -> "1", "start" -> 102, "genotypes" -> 5)))), 
        Rec("genotypes" -> (variants__D_2genotypes_1, ())))
       

   val clinical__F = 6
   val clinical__D = (List((clinical__F, List( 
       Rec("sample" -> "one", "iscase" -> "case"), 
        Rec("sample" -> "two", "iscase" -> "case"), 
         Rec("sample" -> "three", "iscase" -> "control"), 
          Rec("sample" -> "four", "iscase" -> "control")))), ())

   val format2Spark = s"""
      |val cases__F = 1
      |val cases__D_1 = spark.sparkContext.parallelize(List((1, List(Case("control"), Case("case")))))
      |val clinical__F = 2
      |val clinical__D_1 = spark.sparkContext.parallelize(List((2, List(
      |  Clinical("one", "case"), Clinical("two", "case"), Clinical("three", "control"), Clinical("four", "control")))))
      |val variants = spark.sparkContext.parallelize(List(
      |  Variant("1", 100, List(Genotype("one", 0), Genotype("two", 1), Genotype("three", 2), Genotype("four", 0))),
      |  Variant("1", 101, List(Genotype("one", 1), Genotype("two", 1), Genotype("three", 0), Genotype("four", 1))),
      |  Variant("1", 102, List(Genotype("one", 2), Genotype("two", 0), Genotype("three", 1), Genotype("four", 2)))))
      |val variants__F = 2
      |val variants__D_1 = variants.map{ case v => VariantFlat(v.contig, v.start, v.hashCode) }
      |val variants__Dgenotypes_1 = variants.map{ case v => (v.hashCode, v.genotypes.map{ case g => Genotype(g.sample, g.call)})}
    """.stripMargin 

}

object GenomicTests {

  val nrc = new ShredNRC{}
  import nrc._

  val relI = BagVarRef(VarDef("cases", BagType(GenomicRelations.casetype)))
  val idef = VarDef("i", GenomicRelations.casetype)
  val iref = TupleVarRef(idef)
  
  val relV = BagVarRef(VarDef("variants", BagType(GenomicRelations.varianttype)))
  val vdef = VarDef("v", GenomicRelations.varianttype)
  val vref = TupleVarRef(vdef)
  val gdef = VarDef("g", GenomicRelations.genotype)
  val gref = TupleVarRef(gdef)

  val relC = BagVarRef(VarDef("clinical", BagType(GenomicRelations.clintype)))
  val cdef = VarDef("c", GenomicRelations.clintype)
  val cref = TupleVarRef(cdef)

  // designed to construct a set of labelled points 
  // def lmm() = // some function that will do binary classification 
  // val v_sig = variants.map( v => (v.contig, v.start, 
  //              lmm(v.genotypes.groupBy(_.label).map{ case (k,v) => LabelPoint(k, v.map(_.call)) })))  
  val q6 = ForeachUnion(vdef, relV, 
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), 
              "dataset" -> ForeachUnion(idef, relI, 
                  Singleton(Tuple("label" -> iref("iscase"), "features" ->
                    ForeachUnion(cdef, relC, 
                      IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                        ForeachUnion(gdef, BagProject(vref, "genotypes"),
                          IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                            Singleton(Tuple("call" -> gref("call"))))))))))))) 

  val q1 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"), 
              "counts" -> ForeachUnion(idef, relI, 
                Singleton(Tuple("iscase" -> iref("iscase"), "altcnt" -> 
                  Total(ForeachUnion(cdef, relC, 
                    IfThenElse(Cmp(OpEq, cref("iscase"), iref("iscase")),
                      ForeachUnion(gdef, BagProject(vref, "genotypes"),
                        IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                          IfThenElse(Cmp(OpEq, gref("call"), Const(1, IntType)),
                            Singleton(Tuple("count" -> Const("alt", StringType))), 
                            IfThenElse(Cmp(OpEq, gref("call"), Const(2, IntType)),
                              WeightedSingleton(Tuple("count" -> Const("alt", StringType)), NumericConst(2, IntType))))).asInstanceOf[BagExpr]))))))))))

  // count the number of genotypes
  val q2 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  ForeachUnion(cdef, relC, 
                                    IfThenElse(Cmp(OpEq, cref("sample"), gref("sample")),
                                      Singleton(Tuple("alt" -> Const("alt", StringType))))))))))
  
  // count the number of genotypes for case, but not control 
  val q3 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  ForeachUnion(cdef, relC, 
                                    IfThenElse(
                                      And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                      Singleton(Tuple("alt" -> Const("alt", StringType))))))))))

  // count the number of mutations that have at least one alternate allele for case, but not control 
  val q4 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  IfThenElse(Cmp(OpGt, gref("call"), Const(0, IntType)),
                                    ForeachUnion(cdef, relC, 
                                      IfThenElse(
                                        And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                        Singleton(Tuple("alt" -> Const("alt", StringType)))))))))))

  val q5 = ForeachUnion(vdef, relV,
            Singleton(Tuple("contig" -> vref("contig"), "start" -> vref("start"),
              "counts" -> Total(ForeachUnion(gdef, BagProject(vref, "genotypes"), 
                                  IfThenElse(Cmp(OpGt, gref("call"), Const(0, IntType)),
                                    ForeachUnion(cdef, relC, 
                                      IfThenElse(
                                        And(Cmp(OpEq, cref("sample"), gref("sample")),
                                          Cmp(OpEq, cref("iscase"), Const("case", StringType))),
                                        IfThenElse(
                                          Cmp(OpEq, gref("call"), Const(1, IntType)),
                                          Singleton(Tuple("alt" -> Const("alt", StringType))),
                                          WeightedSingleton(Tuple("alt" -> Const("alt", StringType)), NumericConst(2, IntType)))).asInstanceOf[BagExpr])))))))



}
