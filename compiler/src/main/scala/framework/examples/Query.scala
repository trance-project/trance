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
  val baseTag: String = "_2"

  val name: String
  def loadTables(shred: Boolean = false, skew: Boolean = false): String 

  /** Standard Pipeline Runners **/

  val program: Program

  // nrc to plan language
  def calculus: CExpr = {
    println("RUNNING STANDARD PIPELINE:\n")
    println(quote(program))
    translate(program)
  }

  def normalize: CExpr = {
    val nc = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
    //println(Printer.quote(nc))
    nc
  }
  
  def unnest: CExpr = Unnester.unnest(this.normalize)(Map(), Map(), None, baseTag)

  def anf(optimizationLevel: Int = 2): CExpr = {
    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val un = this.unnest
    // println(un)
    optimizationLevel match {
      case 0 => anfBase.anf(anfer.finalize(un).asInstanceOf[anfBase.Rep])
      case 1 => anfBase.anf(anfer.finalize(Optimizer.applyPush(un)).asInstanceOf[anfBase.Rep])
      case _ => 
        println("\n after opt \n")
        val plan = Optimizer.applyAll(un)
        println(Printer.quote(plan))
        val anfed = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
        println(Printer.quote(anfed))
        anfed 
    }
  }


  /** Shredded Pipeline Runners **/

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
    val inputInitPlan = Unnester.unnest(inputC)(Map(), Map(), None, baseTag)
    val inputPlan = Optimizer.push(compiler.finalize(inputInitPlan).asInstanceOf[CExpr])
    println(Printer.quote(inputPlan))
    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val iPlan = anfBase.anf(anfer.finalize(inputPlan).asInstanceOf[anfBase.Rep])

    // shredded pipeline plan for query
    println("\nRUNNING SHREDDED PIPELINE:\n")
    println(quote(this.program))
    println(quote(mat.program))
    val calc = normalizer.finalize(translate(mat.program)).asInstanceOf[CExpr]
    val initPlan = Unnester.unnest(calc)(Map(), Map(), None, baseTag)
    val plan = Optimizer.push(compiler.finalize(initPlan).asInstanceOf[CExpr])
    println(Printer.quote(plan))
    // println(plan)
    val sanfBase = new BaseOperatorANF{}
    val sanfer = new Finalizer(sanfBase)
    val splan = sanfBase.anf(sanfer.finalize(plan).asInstanceOf[sanfBase.Rep])

    // shredded pipeline unshredding plan for query
    val usplan = if (unshredRun){
      val unshredProg = unshred(optShredded, mat.ctx)
      val uncalc = normalizer.finalize(translate(unshredProg)).asInstanceOf[CExpr]
      val uinitPlan = Unnester.unnest(uncalc)(Map(), Map(), None, baseTag)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]
      println(Printer.quote(uplan))
      val uanfBase = new BaseOperatorANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uplan).asInstanceOf[uanfBase.Rep])
    }else CUnit

    (iPlan, splan, usplan)

  }

  def shred(eliminateDomains: Boolean = true): (Program, Program) = {
      println("INPUT QUERY:\n")
      println(quote(program))
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      val materializedProgram = materialize(optShredded, eliminateDomains = eliminateDomains)
      val unshredProg = unshred(optShredded, materializedProgram.ctx)
      (materializedProgram.program, unshredProg)
    }

  /** Shred plan for batch operator compilation **/
  def shredBatchPlan(unshredRun: Boolean = false, eliminateDomains: Boolean = true, anfed: Boolean = true): (CExpr, CExpr) = {
    val compiler = new Finalizer(new ShredOptimizer{})
    // shredded pipeline for query
    val (matProg, ushred) = shred(eliminateDomains)
    println("RUNNING SHREDDED PIPELINE:\n")
    println(quote(matProg))
    val ncalc = normalizer.finalize(translate(matProg)).asInstanceOf[CExpr]
    // println(ncalc)
    val initPlan = Unnester.unnest(ncalc)(Map(), Map(), None, baseTag)
    // println("plan before")
    // println(Printer.quote(initPlan))
    val plan = Optimizer.push(compiler.finalize(initPlan).asInstanceOf[CExpr])
    println(Printer.quote(plan))
    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val qplan = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])

    //shredded pipeline for unshredding 
    val usplan = if (unshredRun){
      val uncalc = normalizer.finalize(translate(ushred)).asInstanceOf[CExpr]
      val uinitPlan = Unnester.unnest(uncalc)(Map(), Map(), None, baseTag)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]
      println(Printer.quote(uplan))
      val uanfBase = new BaseOperatorANF{}
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

  /** misc utils **/
  def varset(n1: String, n2: String, e: BagExpr): (BagVarRef, TupleVarRef) =
    (BagVarRef(n1, e.tp), TupleVarRef(n2, e.tp.tp))
}
