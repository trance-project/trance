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
/**
  * Helper methods to run a query pipeline
trait PipelineRunner extends Printer {
    
  val translator = new NRCTranslator {}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val anfbase = new BaseANF{}
  val anfer = new Finalizer(anfbase)

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

  def toAnf(query: Expr): CExpr = {
    val nq = toNormalizedCalc(query)
    anfbase.anf(anfer.finalize(nq))
  }

}

trait ShredPipelineRunner extends Printer
  with Linearization 
  with Shredding
  with Optimizer {

  val translator = new NRCTranslator {}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val anfbase = new BaseANF{}
  val anfer = new Finalizer(anfbase)

  def toCalculus(query: Expr): CExpr = {
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

  def toNormalizedCalc(query: Expr): CExpr = {
    println("\nCalculus:\n")
    val cq = toCalculus(query)
    println(Printer.quote(cq))
    println("\nNormalized:\n")
    val nq = normalizer.finalize(cq)
    println(Printer.quote(nq))
    nq
  }

  def toAnf(query: Expr): CExpr = {
    val nq = toNormalizedCalc(query)
    anfbase.anf(anfer.finalize(nq))
  }

}*/
