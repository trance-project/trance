package shredding.wmcc

import shredding.nrc._

trait PipelineRunner extends Linearization 
  with Shredding
  with Printer
  with Optimizer 
  with NRCTranslator {

  def shredPipeline(query: Expr): CExpr = {
    println("\nQuery:\n")
    println(quote(query))
    val sq = shred(query)
    println("\nShredded:\n")
    println(quote(sq))
    val lsq = linearize(sq)
    println("\nLinearized:\n")
    println(quote(lsq))
    translate(lsq)
  }

}
