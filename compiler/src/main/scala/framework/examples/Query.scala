package framework.examples

import framework.common.Type
import framework.nrc._
import framework.plans._

/** Base functionality of a query, which allows 
  * a query to be easily ran through various stages o
  * of the pipeline.
  */

trait Query extends Materialization
  with MaterializeNRC
  with Printer
  with Shredding
  with NRCTranslator {

  val normalizer = new Finalizer(new BaseNormalizer{})

  val name: String

  /** schema **/
  def inputTables: Set[String] = Set()
  val loaderName: String = "loader"

  // table name to loader function
  def loaders: Map[String, String] = Map()

  def loadTable(tbl: String, eval: String, shred: Boolean = false): String = {
    val tblName = if (shred) s"IBag_${tbl}__D" else tbl
    s"""|val $tblName = $loaderName.${loaders(tbl)}
        |$tblName.cache
        |$tblName.$eval
        |""".stripMargin
  }

  def loadTables(tbls: Set[String], eval: String, shred: Boolean = false): String = {
      s"""|val $loaderName = TPCHLoader(spark)
          |${loaders.filter(f => tbls(f._1)).map(f => loadTable(f._1, eval, shred)).mkString("\n")}
          |""".stripMargin
  }


  def inputs(tmap: Map[String, String]): String
  def inputTypes(shred: Boolean = false): Map[Type, String]
  def headerTypes(shred: Boolean = false): List[String]

  /** Standard Pipeline Runners **/

  val program: Program

  // nrc to plan language
  def calculus: CExpr = {
    println("RUNNING STANDARD PIPELINE:\n")
    println(quote(program))
    translate(program)
  }

  def normalize: CExpr = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
  
  def batchUnnest: CExpr = {
    val initPlan = BatchUnnester.unnest(this.normalize)(Map(), Map(), None)
    // println("PLAN BEFORE")
    // println(Printer.quote(initPlan))
    // arbitrary pass through compilation phase for testing compiler
    val compiler = new Finalizer(new BaseCompiler{})
    val plan = compiler.finalize(initPlan).asInstanceOf[CExpr]
    println("PLAN")
    println(Printer.quote(plan))
    plan
  }
  def unnestNoOpt: CExpr = Unnester.unnest(this.normalize)(Nil, Nil, None)
  def unnest: CExpr = Optimizer.applyAll(unnestNoOpt)

  def anf(batch: Boolean = true, optimizationLevel: Int = 2): CExpr = {
    val anfBase = if (batch) new BaseDFANF{} else new BaseANF{}
    val anfer = new Finalizer(anfBase)
    optimizationLevel match {
      case y if batch => 
        val optimized = BatchOptimizer.push(this.batchUnnest)
        println(Printer.quote(optimized))
        anfBase.anf(anfer.finalize(optimized).asInstanceOf[anfBase.Rep])
      case 0 => anfBase.anf(anfer.finalize(this.unnestNoOpt).asInstanceOf[anfBase.Rep])
      case 1 => anfBase.anf(anfer.finalize(Optimizer.projectOnly(unnestNoOpt)).asInstanceOf[anfBase.Rep])
      case _ => anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
    }
  }


  /** Shredded Pipeline Runners **/

  def shredWithInput(input: Query, unshredRun: Boolean = false, eliminateDomains: Boolean = true): (CExpr, CExpr, CExpr) = {
    // materialize input
    val (inputShredded, inputShreddedCtx) = shredCtx(input.program.asInstanceOf[Program])
    val matInput = materialize(optimize(inputShredded), eliminateDomains = eliminateDomains)
    val (shredded, _) = shredCtx(program, inputShreddedCtx)
    val optShredded = optimize(shredded)
    val mat = materialize(optShredded, matInput.ctx, eliminateDomains = eliminateDomains)

    // shredded pipeline plan for input
    // println("RUNNING SHREDDED PIPELINE:\n")
    // println(quote(matInput.program))
    val inputC = normalizer.finalize(translate(matInput.program)).asInstanceOf[CExpr]
    val inputInitPlan = Unnester.unnest(inputC)(Nil, Nil, None)
    val inputOptPlan = Optimizer.applyAll(inputInitPlan)
    val anfBase = new BaseDFANF{}
    val anfer = new Finalizer(anfBase)
    val inputPlan = anfBase.anf(anfer.finalize(inputOptPlan).asInstanceOf[anfBase.Rep])

    // shredded pipeline plan for query
    println("\nRUNNING SHREDDED PIPELINE:\n")
    println(quote(this.program))
    println(quote(mat.program))
    val calc = normalizer.finalize(translate(mat.program)).asInstanceOf[CExpr]
    val initPlan = Unnester.unnest(calc)(Nil, Nil, None)
    val optPlan = Optimizer.applyAll(initPlan)
    val sanfBase = new BaseDFANF{}
    val sanfer = new Finalizer(sanfBase)
    val splan = sanfBase.anf(sanfer.finalize(optPlan).asInstanceOf[sanfBase.Rep])

    // shredded pipeline unshredding plan for query
    val usplan = if (unshredRun){
      val unshredProg = unshred(optShredded, mat.ctx)
      val uncalc = normalizer.finalize(translate(unshredProg)).asInstanceOf[CExpr]
      val uinitPlan = Unnester.unnest(uncalc)(Nil, Nil, None)
      val uoptPlan = Optimizer.applyAll(uinitPlan)
      val uanfBase = new BaseDFANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uoptPlan).asInstanceOf[uanfBase.Rep])
    }else CUnit

    (inputPlan, splan, usplan)

  }

  def shredBatchWithInput(input: Query, unshredRun: Boolean = false, eliminateDomains: Boolean = true): (CExpr, CExpr, CExpr) = {
    val compiler = new Finalizer(new ShredOptimizer{})

    // materialize input
    val (inputShredded, inputShreddedCtx) = shredCtx(input.program.asInstanceOf[Program])
    val matInput = materialize(optimize(inputShredded), eliminateDomains = eliminateDomains)
    val (shredded, _) = shredCtx(program, inputShreddedCtx)
    val optShredded = optimize(shredded)
    val mat = materialize(optShredded, matInput.ctx, eliminateDomains = eliminateDomains)

    // shredded pipeline plan for input
    // println("RUNNING SHREDDED PIPELINE:\n")
    // println(quote(matInput.program))
    val inputC = normalizer.finalize(translate(matInput.program)).asInstanceOf[CExpr]
    val inputInitPlan = BatchUnnester.unnest(inputC)(Map(), Map(), None)
    val inputPlan = BatchOptimizer.push(compiler.finalize(inputInitPlan).asInstanceOf[CExpr])
    println(Printer.quote(inputPlan))
    val anfBase = new BaseDFANF{}
    val anfer = new Finalizer(anfBase)
    val iPlan = anfBase.anf(anfer.finalize(inputPlan).asInstanceOf[anfBase.Rep])

    // shredded pipeline plan for query
    println("\nRUNNING SHREDDED PIPELINE:\n")
    println(quote(this.program))
    println(quote(mat.program))
    val calc = normalizer.finalize(translate(mat.program)).asInstanceOf[CExpr]
    val initPlan = BatchUnnester.unnest(calc)(Map(), Map(), None)
    val plan = BatchOptimizer.push(compiler.finalize(initPlan).asInstanceOf[CExpr])
    println(Printer.quote(plan))
    val sanfBase = new BaseDFANF{}
    val sanfer = new Finalizer(sanfBase)
    val splan = sanfBase.anf(sanfer.finalize(plan).asInstanceOf[sanfBase.Rep])

    // shredded pipeline unshredding plan for query
    val usplan = if (unshredRun){
      val unshredProg = unshred(optShredded, mat.ctx)
      val uncalc = normalizer.finalize(translate(unshredProg)).asInstanceOf[CExpr]
      val uinitPlan = BatchUnnester.unnest(uncalc)(Map(), Map(), None)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]
      println(Printer.quote(uplan))
      val uanfBase = new BaseDFANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uplan).asInstanceOf[uanfBase.Rep])
    }else CUnit

    (iPlan, splan, usplan)

  }

  def shred(eliminateDomains: Boolean = true): (Program, Program) = {
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      val materializedProgram = materialize(optShredded, eliminateDomains = eliminateDomains)
      val unshredProg = unshred(optShredded, materializedProgram.ctx)
      (materializedProgram.program, unshredProg)
    }

  /** Shred plan for tuple at a time compilation **/
  def shredPlan(unshredRun: Boolean = false, eliminateDomains: Boolean = true, anfed: Boolean = true): (CExpr, CExpr) = {
      
      // shredded pipeline for query
      val (matProg, ushred) = shred(eliminateDomains)
      println("RUNNING SHREDDED PIPELINE:\n")
      println(quote(matProg))
      val ncalc = normalizer.finalize(translate(matProg)).asInstanceOf[CExpr]
      val initPlan = Unnester.unnest(ncalc)(Nil, Nil, None)
      val optPlan = Optimizer.applyAll(initPlan)
      val qplan = if (anfed) {
        val anfBase = new BaseDFANF{}
        val anfer = new Finalizer(anfBase)
        anfBase.anf(anfer.finalize(optPlan).asInstanceOf[anfBase.Rep])
      }else optPlan

      //shredded pipeline for unshredding 
      val usplan = if (unshredRun){
        val uncalc = normalizer.finalize(translate(ushred)).asInstanceOf[CExpr]
        val uinitPlan = Unnester.unnest(uncalc)(Nil, Nil, None)
        val uoptPlan = Optimizer.applyAll(uinitPlan)
        val uanfBase = new BaseDFANF{}
        val uanfer = new Finalizer(uanfBase)
        uanfBase.anf(uanfer.finalize(uoptPlan).asInstanceOf[uanfBase.Rep])
      }else CUnit
      (qplan, usplan)
  }

  /** Shred plan for batch operator compilation **/
  def shredBatchPlan(unshredRun: Boolean = false, eliminateDomains: Boolean = true, anfed: Boolean = true): (CExpr, CExpr) = {
    val compiler = new Finalizer(new ShredOptimizer{})
    // shredded pipeline for query
    val (matProg, ushred) = shred(eliminateDomains)
    println("RUNNING SHREDDED PIPELINE:\n")
    println(quote(matProg))
    val ncalc = normalizer.finalize(translate(matProg)).asInstanceOf[CExpr]
    val initPlan = BatchUnnester.unnest(ncalc)(Map(), Map(), None)
    val plan = BatchOptimizer.push(compiler.finalize(initPlan).asInstanceOf[CExpr])
    println(Printer.quote(plan))
    val anfBase = new BaseDFANF{}
    val anfer = new Finalizer(anfBase)
    val qplan = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])

    //shredded pipeline for unshredding 
    val usplan = if (unshredRun){
      val uncalc = normalizer.finalize(translate(ushred)).asInstanceOf[CExpr]
      val uinitPlan = BatchUnnester.unnest(uncalc)(Map(), Map(), None)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]
      println(Printer.quote(uplan))
      val uanfBase = new BaseDFANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uplan).asInstanceOf[uanfBase.Rep])
    }else CUnit
    (qplan, usplan)
  }


  def shred: (ShredProgram, MaterializedProgram) = {
    val sprog = optimize(shred(program))
    (sprog, materialize(sprog))
  }

  def unshred: Program = {
    val shredset = this.shred
    unshred(shredset._1, shredset._2.ctx)
  }


  /** Shredded pipeline for Scala code generation - deprecated for Spark **/

  def shredPlan: CExpr = {
    val seq = this.shred._2.program
    val ctrans = translate(seq)
    val shredded = normalizer.finalize(ctrans).asInstanceOf[CExpr] 
    val initPlan = Unnester.unnest(shredded)(Nil, Nil, None)
    Optimizer.applyAll(initPlan)
  }

  def shredPlanNoOpt: CExpr = {
    val seq = this.shred._2.program
    val ctrans = translate(seq)
    val shredded = normalizer.finalize(ctrans).asInstanceOf[CExpr] 
    Unnester.unnest(shredded)(Nil, Nil, None)
  }
 
  def shredANF: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.shredPlan).asInstanceOf[anfBase.Rep])
  }
 
  def unshredPlan: CExpr = {
    val c = translate(this.unshred)
    val unshredded = normalizer.finalize(c).asInstanceOf[CExpr] 
    val initPlan = Unnester.unnest(unshredded)(Nil, Nil, None)
    Optimizer.applyAll(initPlan)
  }

  def unshredANF: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.unshredPlan).asInstanceOf[anfBase.Rep])
  }
  
  def indexedDict: List[String] = Nil

  /** misc utils **/
  def varset(n1: String, n2: String, e: BagExpr): (BagVarRef, TupleVarRef) =
    (BagVarRef(n1, e.tp), TupleVarRef(n2, e.tp.tp))
}
