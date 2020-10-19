package framework.plans

import framework.examples.tpch._
import framework.examples.normalize._
import framework.nrc.{Printer => NRCPrinter}
import framework.nrc.MaterializeNRC


object App extends NRCPrinter with MaterializeNRC {

  def normalizationTests(): Unit = {
    val compiler = new NRCFinalizer(new BaseNRCCompiler{})
   
    val q1 = Program(Test2.program.statements.map(a => Assignment(a.name, 
      compiler.finalize(a.rhs.asInstanceOf[compiler.Expr]).asInstanceOf[Expr])))
    
    println(quote(q1))

    val normalizer = new NRCFinalizer(new BaseNRCNormalizer{})

    val q2 = compiler.finalize(NormalizationTests.query2.asInstanceOf[compiler.Expr])
    // println(quote(q2.asInstanceOf[Expr]))

    val nq2 = normalizer.finalize(NormalizationTests.query2.asInstanceOf[normalizer.Expr])
    // println(quote(nq2.asInstanceOf[Expr]))

    val q3 = compiler.finalize(NormalizationTests.query2.asInstanceOf[compiler.Expr])
    // println(quote(q3.asInstanceOf[Expr]))

    val nq3 = normalizer.finalize(NormalizationTests.query5.asInstanceOf[normalizer.Expr])
    // println(quote(nq3.asInstanceOf[Expr]))

    val q6 = compiler.finalize(NormalizationTests.query6.asInstanceOf[compiler.Expr])
    println(quote(q6.asInstanceOf[Expr]))

    val nq6 = normalizer.finalize(NormalizationTests.query6.asInstanceOf[normalizer.Expr])
    println(quote(nq6.asInstanceOf[Expr]))
    
    val q8 = compiler.finalize(NormalizationTests.query8.asInstanceOf[compiler.Expr])
    println(quote(q8.asInstanceOf[Expr]))

    val nq8 = normalizer.finalize(NormalizationTests.query8.asInstanceOf[normalizer.Expr])
    println(quote(nq8.asInstanceOf[Expr]))
  }

  def main(args: Array[String]){
    // normalizationTests()

    // println(quote(TPCHQuery1.normalizeNew.asInstanceOf[Program]))

    // println(quote(TPCHQuery1.shredNew.asInstanceOf[Program]))
    // println("Flat to Nested:\n")
    // println(quote(TPCHQuery1.program.asInstanceOf[Program]))
    val planner = NRCUnnester

    // println("standard pipeline")
    // val u = planner.unnest(TPCHQuery1.normalizeNew.asInstanceOf[planner.Program])
    // println(u)

    // println("shredded pipeline")
    // val (us, usp) = TPCHQuery1.shredNew
    // println(quote(us.asInstanceOf[Program]))
    // println(planner.unnest(us.asInstanceOf[planner.Program]))

    // println("unshredding")
    // println(quote(usp.asInstanceOf[Program]))
    // println(planner.unnest(usp.asInstanceOf[planner.Program]))

    println("Nested to Nested:\n")

    println(quote(Test2NN.program.asInstanceOf[Program]))

    println("standard pipeline")
    val u2 = planner.unnest(Test2NN.normalizeNew.asInstanceOf[planner.Program])
    println(u2)

    println("shredded pipeline")
    val (us2, usp2) = Test2NN.shredNew
    println(quote(us2.asInstanceOf[Program]))
    println(planner.unnest(us2.asInstanceOf[planner.Program]))

    println("unshredding")
    println(quote(usp2.asInstanceOf[Program]))
    println(planner.unnest(usp2.asInstanceOf[planner.Program]))

  }

}
