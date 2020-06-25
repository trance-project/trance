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

/*
* Example OddRatio Test Application
* */
object TestApp1 extends App {
    override def main(args: Array[String]){
        AppWriter.flatDataset(OddsRatio, "test")
//        AppWriter.shredDataset(OddsRatio, "test1", unshred = true)
    }
}


object Optimize1 extends App {
  AppWriter.flatDataset(Step1, "test") // todo: need to revisit this: key not found: v2__F_samples
//  AppWriter.shredDataset(Step1, label = "test")
}


/** Example Genomic Test Application
  *
  */
object TestApp2 extends App {

    override def main(args: Array[String]){
      AppWriter.flatDataset(Step2, "test") // todo: need to revisit this: key not found: v2__F_samples
//      AppWriter.shredDataset(Step2, label = "test")
      
    }
}


object TestApp3 extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(Gene_Burden, "test")
//    AppWriter.shredDataset(Gene_Burden, "test")

  }
}


object TestApp4 extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(Pathway_Burden, "test")
//    AppWriter.shredDataset(Pathway_Burden, "test")
//    AppWriter.flatDataset(Pathway_Burden_Flatten, "test")
  }
}

object TestApp5 extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(Clinical_Pathway_Burden, "test")
    AppWriter.flatDataset(Clinical_Pathway_Burden_Flatten, "test")
    AppWriter.shredDataset(Clinical_Pathway_Burden, "test")

  }
}


object TestExample1 extends App {

  override def main(args: Array[String]){

    AppWriter.flatDataset(Example1, "test")
//    AppWriter.shredDataset(Example1, "test")

  }
}