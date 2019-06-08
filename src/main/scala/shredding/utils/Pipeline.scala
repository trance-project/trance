package shredding.utils

import shredding.nrc._
import shredding.wmcc._

/**
  * Helper methods to run a query pipeline
  */
trait PipelineRunner extends Printer {
  
  
  val translator = new NRCTranslator {}
  val normalizer = new Finalizer(new BaseNormalizer{})
 
  implicit def toExpr(e: Expr): translator.Expr = e.asInstanceOf[translator.Expr]
  implicit def toCExpr(e: normalizer.target.Rep): CExpr = e.asInstanceOf[CExpr] 

  def toCalculus(query: Expr): CExpr = {
    println("\nQuery:\n")
    println(quote(query))
    translator.translate(query)
  }

  def toNormalizedCalc(query: Expr): CExpr = {
    println("\nCalculus:\n")
    val cq = toCalculus(query)
    println(Printer.quote(cq))
    println("\nNormalized:\n")
    val nq = normalizer.finalize(cq)
    println(Printer.quote(nq))
    nq
  }

}

trait ShredPipelineRunner extends PipelineRunner
  with Linearization 
  with Shredding
  with Optimizer {

  def shredPipeline(query: Expr): CExpr = {
    println("\nQuery:\n")
    println(quote(query))
    val sq = shred(query)
    println("\nShredded:\n")
    println(quote(sq))
    val lsq = linearize(sq)
    println("\nLinearized:\n")
    println(quote(lsq))
    translator.translate(lsq)
  }

}
