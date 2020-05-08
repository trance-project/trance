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
  def unnestNoOpt: CExpr = Unnester.unnest(this.normalize)(Nil, Nil, None)
  def unnest: CExpr = Optimizer.applyAll(unnestNoOpt)

  def anf(optimizationLevel: Int = 2): CExpr = {
    // val anfBase = new BaseANF{}
    val anfBase = new BaseDFANF{}
    val anfer = new Finalizer(anfBase)
    optimizationLevel match {
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

  def shred(eliminateDomains: Boolean = true): (Program, Program) = {
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      val materializedProgram = materialize(optShredded, eliminateDomains = eliminateDomains)
      val unshredProg = unshred(optShredded, materializedProgram.ctx)
      (materializedProgram.program, unshredProg)
    }

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