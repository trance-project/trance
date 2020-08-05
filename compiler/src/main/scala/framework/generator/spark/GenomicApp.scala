package framework.generator.spark

import framework.examples._
import framework.examples.genomic.{OddsRatio}

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
  override def main(args: Array[String]) {
    AppWriter.flatDataset(OddsRatio, "test")
    //        AppWriter.shredDataset(OddsRatio, "test1", unshred = true)
  }
}


object Optimize1 extends App {
  AppWriter.flatDataset(Step1, "test")
  //  AppWriter.shredDataset(Step1, label = "test"
  //
  //  )
}


/** Example Genomic Test Application
  *
  */
object TestApp2 extends App {

  override def main(args: Array[String]) {
    AppWriter.flatDataset(Step2, "test")
    //      AppWriter.shredDataset(Step2, label = "test")

  }
}


object TestApp3 extends App {

  override def main(args: Array[String]) {

    AppWriter.flatDataset(Gene_Burden, "test")
    AppWriter.shredDataset(Gene_Burden, "test")

  }
}


object TestApp4 extends App {

  override def main(args: Array[String]) {

    AppWriter.flatDataset(Pathway_Burden, "test")
    AppWriter.shredDataset(Pathway_Burden, "test")
    //    AppWriter.flatDataset(Pathway_Burden_Flatten, "test")
  }
}

object TestApp5 extends App {

  override def main(args: Array[String]) {

    AppWriter.flatDataset(Clinical_Pathway_Burden, "test")
    AppWriter.flatDataset(Clinical_Pathway_Burden_Flatten, "test")
    AppWriter.shredDataset(Clinical_Pathway_Burden, "test")

  }
}


object TestExample1 extends App {

  override def main(args: Array[String]) {

    AppWriter.flatDataset(Example1, "test")
    //    AppWriter.shredDataset(Example1, "test")

  }
}

object pathway_by_gene_Test extends App {
  override def main(args: Array[String]): Unit = {
    AppWriter.flatDataset(pathway_by_gene, "test")
  }
}

object PathwayBurden_Test extends App {
  override def main(args: Array[String]): Unit = {
    AppWriter.flatDataset(plan2, "test")
    //    AppWriter.shredDataset(plan2, label = "test")
    AppWriter.flatDataset(plan2_1, "test")
  }
}


object PathwayBurden_Test4 extends App {
  override def main(args: Array[String]): Unit = {
    AppWriter.flatDataset(plan4, "test")
    AppWriter.shredDataset(plan4, "test")
    //    AppWriter.shredDataset(plan2_1, label = "test", skew = true)
  }
}

object PathwayBurden_Test5 extends App {
  override def main(args: Array[String]): Unit = {
    AppWriter.flatDataset(plan5, "test")
    AppWriter.shredDataset(plan5, "test", unshred = true)
    //    AppWriter.shredDataset(plan5, label = "test", skew = true)
  }
}

object PathwayBurden_Test6 extends App {
  override def main(args: Array[String]): Unit = {
    //        AppWriter.flatDataset(plan6, "test")
    AppWriter.shredDataset(plan6, "test")
    //        AppWriter.shredDataset(plan6, label = "test", skew = true)
  }
}


object GeneBurdenAlt extends App {
  override def main(args: Array[String]): Unit = {

    AppWriter.flatDataset(geneBurdenAlt, "test")
    AppWriter.shredDataset(geneBurdenAlt, "test")
  }
}


object GeneBurdenAlt2 extends App {
  override def main(args: Array[String]): Unit = {

    AppWriter.flatDataset(geneBurdenAlt2, "test")
    AppWriter.shredDataset(geneBurdenAlt2, "test")
  }
}


object GeneBurdenAlt1WithGeneLoader extends App {
  override def main(args: Array[String]): Unit = {

    AppWriter.flatDataset(geneBurdenAlt_withGenLoader, "test")
    AppWriter.shredDataset(geneBurdenAlt_withGenLoader, "test")
  }
}