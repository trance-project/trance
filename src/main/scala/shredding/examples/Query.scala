package shredding.examples

import shredding.core.{Type, VarDef}
import shredding.nrc.LinearizedNRC
import shredding.wmcc._

trait Query extends NRCTranslator {

  val runner = new PipelineRunner{}
  val normalizer = new Finalizer(new BaseNormalizer{})

  val name: String
  def inputs(tmap: Map[String, String]): String
  def inputTypes(shred: Boolean = false): Map[Type, String]
  def headerTypes(shred: Boolean = false): List[String]

  /** standard query **/
  val query: Expr
  def calculus: CExpr = {val q = translate(query); println(Printer.quote(q)); q}
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
    //println(Printer.quote(this.normalize))
    anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
  }

  /** shred query **/
  def shred: Expr = runner.shredPipelineNew(query.asInstanceOf[runner.Expr]).asInstanceOf[Expr]
  def indexedDict: List[String] = Nil
  def scalculus: CExpr = {val q = translate(shred); println(Printer.quote(q)); q}
  def snormalize: CExpr = {
    val norm = normalizer.finalize(this.scalculus).asInstanceOf[CExpr]
    println(Printer.quote(norm))
    norm
  }
  def sunnest: CExpr = {
    val plan = Optimizer.applyAll(Unnester.unnest(this.snormalize)(Nil, Nil, None))
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
