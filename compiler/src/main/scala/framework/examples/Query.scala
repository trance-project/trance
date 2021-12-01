package framework.examples

import framework.common.Type
import framework.nrc._
import framework.plans._
import framework.optimize.Optimizer
import framework.loader.csv._

/** Base functionality of a query, which allows 
  * a query to be easily ran through various stages o
  * of the pipeline.
  */
trait Query extends Materialization
  with MaterializeNRC
  with Printer
  with Shredding
  with NRCTranslator {

  val letOpt: Boolean = false
  
  val normal = new BaseNormalizer(letOpt)
  val normalizer = new Finalizer(normal)
  val baseTag: String = "_2"

  val name: String
  def loadTables(shred: Boolean = false, skew: Boolean = false): String 

  /** Standard Pipeline Runners **/

  val program: Program

  val opts = Set[MaterializationOption](MOptEliminateDomains)

  // nrc to plan language
  def calculus: CExpr = {
    println("RUNNING STANDARD PIPELINE:\n")
    println(quote(program))
    translate(program)
  }

  def normalize: CExpr = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
  
  def unnest: CExpr = Unnester.unnest(this.normalize)(Map(), Map(), None, baseTag, 0)

  def anf(optimizationLevel: Int = 2, schema: Schema = Schema()): CExpr = {
    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val un = this.unnest
    val optimizer = Optimizer(schema)
    optimizationLevel match {
      case 0 => anfBase.anf(anfer.finalize(un).asInstanceOf[anfBase.Rep])
      case 1 => anfBase.anf(anfer.finalize(optimizer.applyPush(un)).asInstanceOf[anfBase.Rep])
      case _ => 
        val fp = optimizer.applyAll(un)
        anfBase.anf(anfer.finalize(fp).asInstanceOf[anfBase.Rep])
    }
  }

  def optimized(shred: Boolean = false, optLevel: Int = 2, schema: Schema = Schema()): CExpr = {
    val optimizer = Optimizer(schema)
    val compiler = if (shred) new ShredOptimizer {} else new BaseCompiler{}
    val compile = new Finalizer(compiler)
    val unopt = if (shred){
      println("SHREDDING THIS")
      println(quote(program))
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      println("shredded:")
      println(quote(optShredded))
      val materialized = materialize(optShredded, opts)
      val mprogram = materialized.program
      println("materialized:")
      println(quote(mprogram))

      val calc = translate(mprogram)
      val ncalc = normalizer.finalize(calc).asInstanceOf[CExpr]
      Unnester.unnest(ncalc)(Map(), Map(), None, "_2", 0)

    }else this.unnest

    val opt = optLevel match {
      case 0 => unopt
      case 1 => optimizer.applyPush(unopt)
      case _ => optimizer.applyAll(unopt)
    }

    compile.finalize(opt).asInstanceOf[LinearCSet]
  }


  /** Shredded Pipeline Runners **/

  def shredBatchWithInput(input: Query, unshredRun: Boolean = false, eliminateDomains: Boolean = true, optLevel: Int = 2, schema: Schema = Schema()): (CExpr, CExpr, CExpr) = {
    val compiler = new Finalizer(new ShredOptimizer{})
    val optimizer = Optimizer(schema)

    // materialize input
    val (inputShredded, inputShreddedCtx) = shredCtx(input.program.asInstanceOf[Program])
    val matInput = materialize(optimize(inputShredded), opts)
    val (shredded, _) = shredCtx(program, inputShreddedCtx)
    val optShredded = optimize(shredded)
    val mat = materialize(optShredded, matInput.ctx, opts)

    // shredded pipeline plan for input
    // println("RUNNING SHREDDED PIPELINE:\n")
    // println(quote(matInput.program))
    val inputC = normalizer.finalize(translate(matInput.program)).asInstanceOf[CExpr]
    val inputInitPlan = Unnester.unnest(inputC)(Map(), Map(), None, baseTag, 0)
    val inputPlan = optimizer.applyAll(compiler.finalize(inputInitPlan).asInstanceOf[CExpr])

    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val iPlan = anfBase.anf(anfer.finalize(inputPlan).asInstanceOf[anfBase.Rep])

    // shredded pipeline plan for query
    println("\nRUNNING SHREDDED PIPELINE:\n")
    println(quote(this.program))
    println(quote(mat.program))
    val calc = normalizer.finalize(translate(mat.program)).asInstanceOf[CExpr]

    val initPlan = Unnester.unnest(calc)(Map(), Map(), None, baseTag, 0)
    val plan = optLevel match {
      case 0 => compiler.finalize(initPlan).asInstanceOf[CExpr]
      case 1 => optimizer.applyPush(compiler.finalize(initPlan).asInstanceOf[CExpr])
      case _ => optimizer.applyAll(compiler.finalize(initPlan).asInstanceOf[CExpr])
    }

    val sanfBase = new BaseOperatorANF{}
    val sanfer = new Finalizer(sanfBase)
    val splan = sanfBase.anf(sanfer.finalize(plan).asInstanceOf[sanfBase.Rep])

    // shredded pipeline unshredding plan for query
    val usplan = if (unshredRun){
      val unshredProg = unshred(optShredded, mat.ctx)
      val uncalc = normalizer.finalize(translate(unshredProg)).asInstanceOf[CExpr]
      val uinitPlan = Unnester.unnest(uncalc)(Map(), Map(), None, baseTag, 0)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]

      val uanfBase = new BaseOperatorANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uplan).asInstanceOf[uanfBase.Rep])
    }else CUnit

    (iPlan, splan, usplan)

  }

  /** Shred plan for batch operator compilation **/
  def shredBatchPlan(unshredRun: Boolean = false, eliminateDomains: Boolean = true, anfed: Boolean = true, optLevel: Int = 2, schema: Schema = Schema()): (CExpr, CExpr) = {
    val compiler = new Finalizer(new ShredOptimizer{})
    val optimizer = Optimizer(schema)
    // shredded pipeline for query
    // val (matProg, ushred) = shred(eliminateDomains)
    val (shredded, shreddedCtx) = shredCtx(program)
    val optShredded = optimize(shredded)
    println(quote(optShredded))
    val matProg = materialize(optShredded, opts).asInstanceOf[MProgram]
    println("RUNNING SHREDDED PIPELINE:\n")
    println(matProg)

    val bcalc = translate(matProg)
    val ncalc = normalizer.finalize(bcalc).asInstanceOf[CExpr]
    val initPlan = Unnester.unnest(ncalc)(Map(), Map(), None, baseTag, 0)

    val plan = optLevel match {
      case 0 => compiler.finalize(initPlan).asInstanceOf[CExpr]
      case 1 => optimizer.applyPush(compiler.finalize(initPlan).asInstanceOf[CExpr])
      case _ => optimizer.applyAll(compiler.finalize(initPlan).asInstanceOf[CExpr])
    }

    println(Printer.quote(plan))

    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val qplan = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
    //shredded pipeline for unshredding 
    val usplan = if (unshredRun){
      val ushred = unshred(optShredded, matProg.ctx)
      val uncalc = normalizer.finalize(translate(ushred)).asInstanceOf[CExpr]
      val uinitPlan = Unnester.unnest(uncalc)(Map(), Map(), None, baseTag, 0)
      val uplan = compiler.finalize(uinitPlan).asInstanceOf[CExpr]

      val uanfBase = new BaseOperatorANF{}
      val uanfer = new Finalizer(uanfBase)
      uanfBase.anf(uanfer.finalize(uplan).asInstanceOf[uanfBase.Rep])
    }else CUnit
    (qplan, usplan)
  }

  def shred: (ShredProgram, MProgram) = {
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
