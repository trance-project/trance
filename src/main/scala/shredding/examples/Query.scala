package shredding.examples

import shredding.core.Type
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
  def calculus: CExpr = translate(query)
  def normalize: CExpr = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
  def unnest: CExpr = Optimizer.applyAll(Unnester.unnest(this.normalize)(Nil, Nil, None))
  def anf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    println(Printer.quote(this.normalize))
    println(Printer.quote(this.unnest))
    anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
  }

  /** shred query **/
  def shred: Expr = runner.shredPipelineNew(query.asInstanceOf[runner.Expr]).asInstanceOf[Expr]
  def scalculus: CExpr = translate(shred)
  def snormalize: CExpr = normalizer.finalize(this.scalculus).asInstanceOf[CExpr]
  def sunnest: CExpr = Optimizer.applyAll(Unnester.unnest(this.snormalize)(Nil, Nil, None))
  def sanf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    println(Printer.quote(this.snormalize))
    println(Printer.quote(this.sunnest))
    anfBase.anf(anfer.finalize(this.sunnest).asInstanceOf[anfBase.Rep])
  }

}
