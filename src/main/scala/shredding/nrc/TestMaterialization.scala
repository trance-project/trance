package shredding.nrc

import shredding.core.{VarDef}
import shredding.runtime.{Evaluator, ScalaPrinter, ScalaShredding, RuntimeContext}

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

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")
//    println("Materialized program optimized: \n" + quote(optimize(materializedProgram.program)) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
//    println("Unshredded program optimized: \n" + quote(optimize(unshredded)) + "\n")

    val lDict = List[Map[String, Any]](
      Map("l_orderkey" -> 1, "l_partkey" -> 42, "l_quantity" -> 7.0)
    )
    val pDict = List[Map[String, Any]](
      Map("p_partkey" -> 42, "p_name" -> "Kettle", "p_retailprice" -> 12.45)
    )
    val cDict = List[Map[String, Any]](
      Map("c_custkey" -> 10, "c_name" -> "Alice")
    )
    val oDict = List[Map[String, Any]](
      Map("o_orderkey" -> 1, "o_custkey" -> 10, "o_orderdate" -> 20200317)
    )

    val ctx = new RuntimeContext
    ctx.add(VarDef("IDict_L__D", shredding.examples.tpch.TPCHSchema.lineittype), lDict)
    ctx.add(VarDef("IDict_P__D", shredding.examples.tpch.TPCHSchema.parttype), pDict)
    ctx.add(VarDef("IDict_C__D", shredding.examples.tpch.TPCHSchema.customertype), cDict)
    ctx.add(VarDef("IDict_O__D", shredding.examples.tpch.TPCHSchema.orderstype), oDict)

    println("Program eval: ")
    eval(materializedProgram.program, ctx)
    materializedProgram.program.statements.foreach { s =>
      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
    }

    println("Unshredded program eval: ")
    eval(unshredded, ctx)
    program.statements.foreach { s =>
      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
    }

  }

//  run(shredding.examples.tpch.Query1.program.asInstanceOf[Program])
  run(shredding.examples.tpch.Query4.program.asInstanceOf[Program])
}