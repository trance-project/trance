package shredding.calc

import shredding.core._
import shredding.nrc._

trait PipelineRunner extends NRCTranslator 
  with CalcTranslator 
  with Printer {

  object Pipeline extends Serializable {
      
    def run(query: Expr): AlgOp = {
      println("\nQuery:\n")
      println(quote(query))
      val cq = query.translate
      println("\nComprehension Calculus:\n")
      println(cq.quote)
      println("\nNormalized:\n")
      println(cq.normalize.quote)
      val ucq = Unnester.unnest(cq.normalize)
      println("\nAlgebra Plans:\n")
      println(ucq.quote)
      ucq
    } 
  
  } 

}

trait ShredPipelineRunner extends PipelineRunner
  with Linearization 
  with Shredding 
  with Printer
  with Optimizer 
  with NRCTranslator with CalcTranslator {
  
  object ShredPipeline extends Serializable {
    
    def runShred(query: Expr): Expr = {
      println("\nQuery:\n")
      println(quote(query))
      val sq = shred(query)
      println("\nShredded:\n")
      println(quote(sq))
      val lsq = linearize(sq)
      println("\nLinearized:\n")
      println(quote(lsq))
      lsq
    }

    def run(query: Expr): AlgOp = {
      val sq = shred(query)
      println("\nShredded:\n")
      println(quote(sq))
      val lsq = linearize(sq)
      println("\nLinearized:\n")
      println(quote(lsq))
      val cqs = lsq.translate
      println("\nComprehension Calculus:\n")
      println(cqs.quote)
      println("\nNormalized:\n")
      println(cqs.normalize.quote)
      val ucqs = Unnester.unnest(cqs)
      println("\nAlgebra Plans:\n")
      println(ucqs.quote)
      ucqs
    }
 
   def runOptimized(query: Expr): AlgOp = {
      println("\nQuery:\n")
      println(quote(query))
      val sq1 = shred(query)
      println("\nShredded:\n")
      println(quote(sq1))
      val sq = optimize(sq1)
      println("\nShred Optimized:\n")
      println(quote(sq))
      val lsq = linearize(sq)
      println("\nLinearized:\n")
      println(quote(lsq))
      val cqs = lsq.translate
      println("\nComprehension Calculus:\n")
      println(cqs.quote)
      println("\nNormalized:\n")
      println(cqs.normalize.quote)
      val ucqs = Unnester.unnest(cqs)
      println("\nAlgebra Plans:\n")
      println(ucqs.quote)
      ucqs
    }
 
  }

}
