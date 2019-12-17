package shredding.wmcc

import shredding.core._
import shredding.nrc._

trait PipelineRunner extends Linearization 
  with Shredding
  with Printer
  with Optimizer 
  with NRCTranslator {

  def shredPipelineNew(query: Expr, domains: Boolean = false): Expr = query match {
    case Sequence(fs) => Sequence(fs.map{
      case Named(n, e1) => Named(n, shredPipelineNew(e1, domains))
      case e1 => shredPipelineNew(e1, domains) 
    })
    case _ => 
      println("\nQuery:\n")
      println(quote(query))
      //val nq = nestingRewrite(query)
      //println("\nRewrite:\n")
      //println(quote(nq))
      val sq = shred(query)
      //println("\nShredded:\n")
      //println(quote(sq))
      //println("\nOptimized:\n")
      val sqo = optimize(sq)
      //println(quote(sqo))
      val lsq = if (domains) linearize(sqo) else linearizeNoDomains(sqo)
      println("\nLinearized:\n")
      println(quote(lsq))
      lsq
  }

  def shredPipeline(query: Expr): CExpr = {
      println("\nQuery:\n")
      println(quote(query))
      val sq = shred(query)
      println("\nShredded:\n")
      println(quote(sq))
      //println("\nOptimized:\n")
      val sqo = optimize(sq)
      //println(quote(sqo))
      val lsq = linearize(sqo)
      println("\nLinearized:\n")
      println(quote(lsq))
      translate(lsq)
  }

}
