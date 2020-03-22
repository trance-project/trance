package shredding.examples

import shredding.core.Type
import shredding.nrc._
import shredding.wmcc._

trait Query extends Materialization
  with MaterializeNRC
  with Printer
  with Shredding
  with NRCTranslator {

  val runner = new PipelineRunner{}
  val normalizer = new Finalizer(new BaseNormalizer{})

  val name: String
  def inputs(tmap: Map[String, String]): String
  def inputTypes(shred: Boolean = false): Map[Type, String]
  def headerTypes(shred: Boolean = false): List[String]

  /** standard query program **/
  val program: Program
  def calculus: CExpr = {val q = translate(program); println(Printer.quote(q)); q}
  def normalize: CExpr = {
    val norm = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
    // println(Printer.quote(norm))
    norm
  }

  def unnestOnly: CExpr = {
    Unnester.unnest(this.normalize)(Nil, Nil, None)
  }

  def unnest: CExpr = {
    val plan = Optimizer.applyAll(unnestOnly)
    println(Printer.quote(plan))
    plan
  }

  def anf(optimizationLevel: Int = 2): CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    optimizationLevel match {
      case 0 => 
        val plan = Optimizer.indexOnly(unnestOnly)
        println(Printer.quote(plan))
        anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
      case 1 => 
        val plan = Optimizer.projectOnly(unnestOnly)
        println(Printer.quote(plan))
        anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
      case _ => anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
    }
  }

  /** shred query **/

  def shred: (ShredProgram, MaterializedProgram) = {
    println(quote(program))
    val sprog = optimize(shred(program))
    (sprog, materialize(sprog))
  }

  def shredNoDomains: Program =
    runner.shredPipelineNew(program.asInstanceOf[runner.Program]).program.asInstanceOf[Program]

  def unshred: Program = {
    val shredset = this.shred
    val res = unshred(shredset._1, shredset._2.ctx)
    println(quote(res))
    res
  }

  // with domains
  def shredPlan: CExpr = {
    val seq = this.shred._2.program
    println("shredded program")
    println(quote(seq))
    val ctrans = translate(seq)
    println("translated to calc")
    println(Printer.quote(ctrans))
    val shredded = normalizer.finalize(ctrans).asInstanceOf[CExpr] 
    println("normalized calculus")
    println(Printer.quote(shredded))
    val initPlan = Unnester.unnest(shredded)(Nil, Nil, None)
    val plan = Optimizer.applyAll(initPlan)
    println(Printer.quote(plan))
    plan
  }

  def shredPlanNoOpt: CExpr = {
    val seq = this.shred._2.program
    // println(quote(seq))
    val ctrans = translate(seq)
    // println(Printer.quote(ctrans))
    val shredded = normalizer.finalize(ctrans).asInstanceOf[CExpr] 
    // println(Printer.quote(shredded))
    val initPlan = Unnester.unnest(shredded)(Nil, Nil, None)
    initPlan
  }
 
  def shredANF: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.shredPlan).asInstanceOf[anfBase.Rep])
  }

  def skewPlan: CExpr = {
    // val skewplanner = new Finalizer(new BaseSkewCompiler{})
    // val skewplan = skewplanner.finalize(this.shredANF).asInstanceOf[CExpr]
    // println(Printer.quote(skewplan))
    this.shredANF
  }
 
  def unshredPlan: CExpr = {
    val c = translate(this.unshred)
    println(Printer.quote(c))
    val unshredded = normalizer.finalize(c).asInstanceOf[CExpr] 
    println(Printer.quote(unshredded))
    val initPlan = Unnester.unnest(unshredded)(Nil, Nil, None)
    val plan = Optimizer.applyAll(initPlan)
    println(Printer.quote(plan))
    plan
  }

  def unshredANF: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.unshredPlan).asInstanceOf[anfBase.Rep])
  }
  
  def indexedDict: List[String] = Nil
  // have to change this for different materialization methods
  def scalculus: CExpr = { val q = translate(this.shredNoDomains); println(Printer.quote(q)); q}
  def snormalize: CExpr = {
    val norm = normalizer.finalize(this.scalculus).asInstanceOf[CExpr]
    // println(Printer.quote(norm))
    norm
  }
  def sunnest: CExpr = {
    val initPlan = Unnester.unnest(this.snormalize)(Nil, Nil, None)
    // println(Printer.quote(initPlan))
    val plan = Optimizer.applyAll(initPlan)
    println(Printer.quote(plan))
    plan
  }
  def sanf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.sunnest).asInstanceOf[anfBase.Rep])
  }

  /** misc utils **/
  def varset(n1: String, n2: String, e: BagExpr): (BagVarRef, TupleVarRef) =
    (BagVarRef(n1, e.tp), TupleVarRef(n2, e.tp.tp))
}
