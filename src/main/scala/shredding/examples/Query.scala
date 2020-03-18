package shredding.examples

import shredding.core.{Type, VarDef}
import shredding.nrc._
import shredding.wmcc._

trait Query extends Materializer
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
    println(Printer.quote(norm))
    norm
  }
  def unnest: CExpr = {
    val plan = Optimizer.applyAll(Unnester.unnest(this.normalize)(Nil, Nil, None))
    println(Printer.quote(plan))
    plan
  }
  def anf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    val plan = anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
    //println(Printer.quote(plan))
    plan
  }

  /** shred query **/

  def shred: (ShredProgram, MaterializedProgram) = {
    println(quote(program))
    val sprog = optimize(shred(program))
    (sprog, materialize(sprog))
  }

//  def shred: (ShredExpr, MaterializationInfo) = query match {
//    case Sequence(fs) =>
//      println(quote(query))
//      val exprs = fs.map(expr => optimize(shred(expr)))
//      (exprs.last.asInstanceOf[ShredExpr], materialize(exprs))
//    case _ =>
//      println(quote(query))
//      val expr = optimize(shred(query))
//      (expr, materialize(expr))
//  }

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
    println(quote(seq))
    val ctrans = translate(seq)
    println(Printer.quote(ctrans))
    val shredded = normalizer.finalize(ctrans).asInstanceOf[CExpr] 
    println(Printer.quote(shredded))
    val initPlan = Unnester.unnest(shredded)(Nil, Nil, None)
    //println(Printer.quote(initPlan))
    val plan = Optimizer.applyAll(initPlan)
    println(Printer.quote(plan))
    plan
  }
 
  def shredANF: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.shredPlan).asInstanceOf[anfBase.Rep])
  }
 
  def unshredPlan: CExpr = {
    val unshredded = normalizer.finalize(translate(this.unshred)).asInstanceOf[CExpr] 
    println(Printer.quote(unshredded))
    val initPlan = Unnester.unnest(unshredded)(Nil, Nil, None)
    //println(Printer.quote(initPlan))
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
    println(Printer.quote(norm))
    norm
  }
  def sunnest: CExpr = {
    val initPlan = Unnester.unnest(this.snormalize)(Nil, Nil, None)
    println(Printer.quote(initPlan))
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
  def varset(n1: String, n2: String, e: BagExpr): (VarDef, VarDef, TupleVarRef) = {
    val vd = VarDef(n2, e.tp.tp)
    (VarDef(n1, e.tp), vd, TupleVarRef(vd))
  }
}
