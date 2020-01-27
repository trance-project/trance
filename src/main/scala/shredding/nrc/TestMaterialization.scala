package shredding.nrc

//import shredding.examples.tpch.TPCHQuery1
import shredding.runtime.{Evaluator, ScalaPrinter, ScalaShredding}

object TestMaterialization extends App
  with ShredNRC
  with Shredding
  with ScalaShredding
  with ScalaPrinter
  with Materializer
  with Printer
  with Evaluator
  with Optimizer {

  object Example1 {

    def run(): Unit = {
//      val q = shredding.examples.tpch.Query1.query.asInstanceOf[Expr]

//      val (shredded:ShredExpr, materialized: MaterializationInfo) = q match {
//        case Sequence(fs) =>
//          val exprs = fs.map(expr => optimize(shred(expr)))
//          (exprs.last, materialize(exprs))
//        case _ =>
//          val expr = optimize(shred(q))
//          (expr, materialize(expr))
//      }

      import shredding.examples.optimize.DomainOptExample1

      val q = DomainOptExample1.program(DomainOptExample1.name).rhs.asInstanceOf[Expr]
      val shredded = shred(q)

      println("Query: " + quote(q))
      println("Shredded: " + quote(shredded) + "\n")

      println("Shredded optimized: " + quote(optimize(shredded)) + "\n")
//      println("Materialized: ")
//      println(quote(materialized.seq)+"\n")

//      val unshredded = unshred(shredded, materialized.dictMapper)
//      println("Unshredded: ")
//      println(quote(unshredded))

    }
  }

  Example1.run()
}