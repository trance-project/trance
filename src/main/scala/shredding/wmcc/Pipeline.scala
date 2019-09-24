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
    println("\nOptimized:\n")
    val sqo = optimize(sq)
    println(quote(sqo))
    val lsq = linearize(sqo)
    println("\nLinearized:\n")
    println(quote(lsq))
    translate(lsq)
  }

}
