package shredding.nrc

import shredding.runtime.{Evaluator, ScalaPrinter, ScalaShredding}

object TestMaterialization extends App
  with MaterializeNRC
  with Shredding
  with ScalaShredding
  with ScalaPrinter
  with Materializer
  with Printer
  with Evaluator
  with Optimizer {

  def run(program: Program): Unit = {
    println("Program: \n" + quote(program) + "\n")

    val shredded = shred(program)
    println("Shredded program: \n" + quote(shredded) + "\n")
    println("Shredded program optimized: \n" + quote(optimize(shredded)) + "\n")

//    val materializedProgram = materialize(shredded)
////    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")
//    println("Materialized program optimized: \n" + quote(optimize(materializedProgram.program)) + "\n")
//
//    val unshredded = unshred(shredded, materializedProgram.dictMapper)
////    println("Unshredded program: \n" + quote(unshredded) + "\n")
//    println("Unshredded program optimized: \n" + quote(optimize(unshredded)) + "\n")
  }

//  run(shredding.examples.tpch.Query1.program.asInstanceOf[Program])
  run(shredding.examples.tpch.Query4.program.asInstanceOf[Program])
}