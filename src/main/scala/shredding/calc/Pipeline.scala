package shredding.calc

import shredding.core._
import shredding.nrc._

trait PipelineRunner extends NRCTranslator with CalcTranslator {

  object Pipeline extends Serializable {
      
    def run(query: Expr): AlgOp = {
      val cq = Translator.translate(query)
      println(cq.quote)
      println(cq.normalize.quote)
      val ucq = Unnester.unnest(cq.normalize)
      println(ucq.quote)
      ucq
    } 
  
  } 

}
