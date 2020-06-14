package framework.generator.spark

import framework.examples._
import framework.examples.genomic.OddsRatio

//object TestApp extends App {

//    override def main(args: Array[String]){
//
//        // this should point to the directory of generated code in the executor/spark
//        val pathout = "../executor/spark/src/main/scala/sparkutils/generated/"
//
//        // runs the standard pipeline
////        AppWriter.flatDataset(GenomicQuery1, pathout, "test")
//        AppWriter.flatDataset(GenomicQuery1, "../executor/spark/src/main/scala/sparkutils/generated/", "test")
//        // runs the shredded pipeline
////        AppWriter.shredDataset(GenomicQuery1, pathout, "test", unshred = true)
//    }
//}

//object TestApp extends App {
//    override def main(args: Array[String]){
//
////        val pathout =
//        AppWriter.flatDataset(OddsRatio, "../executor/spark/src/main/scala/sparkutils/generated/", "test")
//
//    }
//}


/** Example Test Application
  *
  */
object TestApp1 extends App {

    override def main(args: Array[String]){
//        AppWriter.flatDataset(GenomicQuery1, "test")
        // focuse on flatDataset for now
        // AppWriter.shredDataset(GenomicQuery1, "test", unshred = true)
        AppWriter.flatDataset(Step2, "test")

    }
}
