package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait ConvergeBase extends Query with VCF with GeneLoader {

  def loadConverge(path: String, shred: Boolean = false, skew: Boolean = false): String = {
    if (shred)
      s"""|val convergeLoader = new ConvergeLoader(spark)
          |val IBag_converge__D = convergeLoader.load("$path")
          |IBag_converge__D.cache
          |IBag_converge__D.count
          |""".stripMargin
    else 
      s"""|val convergeLoader = new ConvergeLoader(spark)
          |val converge = convergeLoader.load("$path")
          |converge.cache
          |converge.count
          |""".stripMargin
  }

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    s"""|${loadVCF("/Users/jac/bioqueries/data/sub.vcf", shred, skew)}
        |${loadGene("/Users/jac/oda-irigs-python/CX_PythonLocal_iRIGS/supporting_files/All_human_genes", shred, skew)}
        |${loadConverge("/Users/jac/converge/converge.csv", shred, skew)}
        |""".stripMargin
  }
  
  val convergeType = TupleType("index" -> IntType,
    "srr_id" -> StringType,
    "id" -> StringType,
    "city" -> StringType,
    "province" -> StringType,
    "hospital_code" -> StringType,
    "iscase" -> IntType,
    "dep_e27" -> IntType, 
    "dep_e29" -> IntType,
    "md_a1" -> IntType,
    "md_a2" -> IntType,
    "md_a3" -> IntType,
    "md_a4" -> IntType,
    "md_a5" -> IntType,
    "md_a6" -> IntType,
    "md_a7" -> IntType,
    "md_a8" -> IntType,
    "md_a9" -> IntType,
    "me" -> IntType,
    "panic" -> IntType,
    "gad" -> IntType,
    "dysthymia" -> IntType,
    "postnatal_d" -> IntType,
    "neuroticism" -> IntType,
    "csa" -> IntType,
    "sle" -> IntType,
    "cold_m" -> IntType,
    "auth_m" -> IntType,
    "prot_m" -> IntType,
    "cold_f" -> IntType,
    "auth_f" -> IntType,
    "prot_f" -> IntType,
    "pms" -> IntType,
    "agora_diag" -> IntType,
    "social_diag" -> IntType,
    "animal_diag" -> IntType,
    "situational_diag" -> IntType,
    "blood_diag" -> IntType,
    "phobia" -> IntType,
    "suicidal_thought" -> IntType,
    "suicidal_plan" -> IntType,
    "suicidal_attempt" -> IntType,
    "dob_y" -> IntType,
    "dob_m" -> IntType,
    "dob_d" -> IntType,
    "age" -> IntType,
    "education" -> IntType,
    "occupation" -> IntType,
    "social_class" -> IntType,
    "marital_status" -> IntType,
    "fh_count" -> IntType,
    "height_clean" -> IntType,
    "weight_clean" -> IntType,
    "bmi" -> DoubleType)

  val converge = BagVarRef("converge", BagType(convergeType))
  val cr = TupleVarRef("sample", convergeType)

}


/** Odds ratio query adapted to the converge dataset **/
object ConvergeStep1 extends ConvergeBase {

  val name = "ConvergeStep1"

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
            ForeachUnion(cr, converge,
              IfThenElse(Cmp(OpEq, gr("g_sample"), cr("id")),
                Singleton(Tuple("iscase" -> cr("iscase"), "refCnt" -> 
                  PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                    PrimitiveIfThenElse(homozyg, Const(0.0, DoubleType), Const(2.0, DoubleType))),
                  "altCnt" -> PrimitiveIfThenElse(heterozyg, Const(1.0, DoubleType),
                    PrimitiveIfThenElse(homozyg, Const(2.0, DoubleType), Const(0.0, DoubleType)))
                  ))))),
          List("iscase"),
          List("refCnt", "altCnt")))))

  val program = Program(Assignment(name, query))
}

object ConvergeStep2 extends ConvergeBase {

  val name = "ConvergeStep2"

  val (cnts, ac) = varset(ConvergeStep1.name, "v2", ConvergeStep1.program(ConvergeStep1.name).varRef.asInstanceOf[BagExpr])
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
            "controlRatio" -> 
              PrimitiveIfThenElse(Cmp(OpEq, ac2("iscase"), Const(0, IntType)),
              ac2("altCnt").asNumeric / ac2("refCnt").asNumeric, Const(0.0, DoubleType)),
            "caseRatio" ->  
              PrimitiveIfThenElse(Cmp(OpEq, ac2("iscase"), Const(1, IntType)),
              ac2("altCnt").asNumeric / ac2("refCnt").asNumeric, Const(0.0, DoubleType))
            )))),
      List("contig", "start", "reference", "alternate"),
      List("controlRatio", "caseRatio"))

  val program = ConvergeStep1.program.asInstanceOf[ConvergeStep2.Program].append(Assignment(name, query))
}

object ConvergeOddsRatio extends ConvergeBase {

  val name = "ConvergeOddsRatio"

  val (cnts, ac) = varset(ConvergeStep2.name, "v3", ConvergeStep2.program(ConvergeStep2.name).varRef.asInstanceOf[BagExpr])
  val query = 
    ForeachUnion(ac, cnts, 
      Singleton(Tuple(
        "contig" -> ac("contig"), 
        "start" -> ac("start"),
        "reference" -> ac("reference"),
        "alternate" -> ac("alternate"),
        "odds" -> ac("caseRatio").asNumeric / ac("controlRatio").asNumeric)))

  val program = ConvergeStep2.program.asInstanceOf[ConvergeOddsRatio.Program].append(Assignment(name, query))
}

