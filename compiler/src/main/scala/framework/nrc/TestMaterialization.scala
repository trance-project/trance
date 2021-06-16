package framework.nrc

import framework.common.VarDef
import framework.runtime.{Evaluator, RuntimeContext, ScalaPrinter, ScalaShredding}
import framework.examples.tpch
import framework.examples.tpch.TPCHSchema
import framework.examples.genomic

object TestMaterialization extends App
  with MaterializeNRC
  with Shredding
  with ScalaShredding
  with ScalaPrinter
  with Materialization
  with Printer
  with Evaluator
  with Optimizer {

  def run(program: Program, opts: Set[MaterializationOption]): Unit = {
    println("Program: \n" + quote(program) + "\n")

    val shredded = shred(program)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgramUnopt = materialize(shredded, opts)

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

    val lDict = List[Map[String, Any]](
      Map("l_orderkey" -> 1, "l_partkey" -> 42, "l_suppkey" -> 789, "l_quantity" -> 7.0)
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
    val sDict = List[Map[String, Any]](
      Map("s_suppkey" -> 789, "s_name" -> "Supplier#1")
    )

    val ctx = new RuntimeContext
    ctx.add(VarDef(inputBagName("Lineitem__D"), TPCHSchema.lineittype), lDict)
    ctx.add(VarDef(inputBagName("Part__D"), TPCHSchema.parttype), pDict)
    ctx.add(VarDef(inputBagName("Customer__D"), TPCHSchema.customertype), cDict)
    ctx.add(VarDef(inputBagName("Order__D"), TPCHSchema.orderstype), oDict)
    ctx.add(VarDef(inputBagName("Supplier__D"), TPCHSchema.suppliertype), sDict)

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

  def runSequential(opts: Set[MaterializationOption]): Unit = {
    val q1 = tpch.Test2.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, shreddedCtx) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

//    val lDict = List[Map[String, Any]](
//      Map("l_orderkey" -> 1, "l_partkey" -> 42, "l_suppkey" -> 789, "l_quantity" -> 7.0)
//    )
//    val pDict = List[Map[String, Any]](
//      Map("p_partkey" -> 42, "p_name" -> "Kettle", "p_retailprice" -> 12.45)
//    )
//    val cDict = List[Map[String, Any]](
//      Map("c_custkey" -> 10, "c_name" -> "Alice")
//    )
//    val oDict = List[Map[String, Any]](
//      Map("o_orderkey" -> 1, "o_custkey" -> 10, "o_orderdate" -> 20200317)
//    )
//    val sDict = List[Map[String, Any]](
//      Map("s_suppkey" -> 789, "s_name" -> "Supplier#1")
//    )

//    val ctx = new RuntimeContext
//    ctx.add(VarDef(inputBagName("Lineitem__D"), TPCHSchema.lineittype), lDict)
//    ctx.add(VarDef(inputBagName("Part__D"), TPCHSchema.parttype), pDict)
//    ctx.add(VarDef(inputBagName("Customer__D"), TPCHSchema.customertype), cDict)
//    ctx.add(VarDef(inputBagName("Order__D"), TPCHSchema.orderstype), oDict)
//    ctx.add(VarDef(inputBagName("Supplier__D"), TPCHSchema.suppliertype), sDict)
//
//    println("Program eval: ")
//    eval(materializedProgram.program, ctx)
//    materializedProgram.program.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }
//
//    println("Unshredded program eval: ")
//    eval(unshredded, ctx)
//    q1.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }

    val q4 = tpch.Query4.program.asInstanceOf[Program]

    println("Program: \n" + quote(q4) + "\n")

    val (shredded4, _) = shredCtx(q4, shreddedCtx)
    println("Shredded program: \n" + quote(shredded4) + "\n")

    val optShredded4 = optimize(shredded4)
    println("Shredded program optimized: \n" + quote(optShredded4) + "\n")

    val materializedProgram4 = materialize(optShredded4, materializedProgram.ctx, opts)
    println("Materialized program: \n" + quote(materializedProgram4.program) + "\n")

    val unshredded4 = unshred(optShredded4, materializedProgram4.ctx)
    println("Unshredded program: \n" + quote(unshredded4) + "\n")

//    println("Program eval: ")
//    eval(materializedProgram4.program, ctx)
//    materializedProgram4.program.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }
//
//    println("Unshredded program eval: ")
//    eval(unshredded4, ctx)
//    q4.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }

  }

  def runSequential2(opts: Set[MaterializationOption]): Unit = {
    val q1 = tpch.Test2Full.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, shreddedCtx) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

//    val lDict = List[Map[String, Any]](
//      Map("l_orderkey" -> 1, "l_partkey" -> 42, "l_suppkey" -> 789, "l_quantity" -> 7.0)
//    )
//    val pDict = List[Map[String, Any]](
//      Map("p_partkey" -> 42, "p_name" -> "Kettle", "p_retailprice" -> 12.45)
//    )
//    val cDict = List[Map[String, Any]](
//      Map("c_custkey" -> 10, "c_name" -> "Alice")
//    )
//    val oDict = List[Map[String, Any]](
//      Map("o_orderkey" -> 1, "o_custkey" -> 10, "o_orderdate" -> 20200317)
//    )
//    val sDict = List[Map[String, Any]](
//      Map("s_suppkey" -> 789, "s_name" -> "Supplier#1")
//    )

//    val ctx = new RuntimeContext
//    ctx.add(VarDef(inputBagName("L__D"), TPCHSchema.lineittype), lDict)
//    ctx.add(VarDef(inputBagName("P__D"), TPCHSchema.parttype), pDict)
//    ctx.add(VarDef(inputBagName("C__D"), TPCHSchema.customertype), cDict)
//    ctx.add(VarDef(inputBagName("O__D"), TPCHSchema.orderstype), oDict)
//    ctx.add(VarDef(inputBagName("S__D"), TPCHSchema.suppliertype), sDict)
//
//    println("Program eval: ")
//    eval(materializedProgram.program, ctx)
//    materializedProgram.program.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }
//
//    println("Unshredded program eval: ")
//    eval(unshredded, ctx)
//    q1.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }

    val q4 = Program(Assignment(tpch.Test2NN.name, tpch.Test2NN.query.asInstanceOf[Expr]))

    println("Program: \n" + quote(q4) + "\n")

    val (shredded4, _) = shredCtx(q4, shreddedCtx)
    println("Shredded program: \n" + quote(shredded4) + "\n")

    val optShredded4 = optimize(shredded4)
    println("Shredded program optimized: \n" + quote(optShredded4) + "\n")

    val materializedProgram4 = materialize(optShredded4, materializedProgram.ctx, opts)
    println("Materialized program: \n" + quote(materializedProgram4.program) + "\n")

    val unshredded4 = unshred(optShredded4, materializedProgram4.ctx)
    println("Unshredded program: \n" + quote(unshredded4) + "\n")

//    println("Program eval: ")
//    eval(materializedProgram4.program, ctx)
//    materializedProgram4.program.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }
//
//    println("Unshredded program eval: ")
//    eval(unshredded4, ctx)
//    q4.statements.foreach { s =>
//      println("  " + s.name + " = " + ctx(VarDef(s.name, s.rhs.tp)))
//    }

  }

  def domainTest(opts: Set[MaterializationOption]): Unit = {
    val q1 = tpch.Query5.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, shreddedCtx) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

    val q4 = tpch.Query6Full.program.asInstanceOf[Program]
    // Program(Assignment(tpch.Query6Full.name, tpch.Query6Full.program.asInstanceOf[Expr]))

    println("Program: \n" + quote(q4) + "\n")

    val (shredded4, _) = shredCtx(q4, shreddedCtx)
    println("Shredded program: \n" + quote(shredded4) + "\n")

    val optShredded4 = optimize(shredded4)
    println("Shredded program optimized: \n" + quote(optShredded4) + "\n")

    val materializedProgram4 = materialize(optShredded4, materializedProgram.ctx, opts)
    println("Materialized program: \n" + quote(materializedProgram4.program) + "\n")

    val unshredded4 = unshred(optShredded4, materializedProgram4.ctx)
    println("Unshredded program: \n" + quote(unshredded4) + "\n")
  }

  // These are multi-attribute label cases, but all attributes 
  // satisfy a domain-optimization requirement.
  def dualConditionLabels(opts: Set[MaterializationOption]): Unit = {
    // first case satisfies If Hoisting and Dict Iteration
    val q1 = genomic.HybridBySample.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  def dualConditionLabels2(opts: Set[MaterializationOption]): Unit = {
    // second case has two Dict Iteration labels (from two different inputs)
    val q2 = genomic.EffectBySample.program.asInstanceOf[Program]

    println("Program: \n" + quote(q2) + "\n")

    val (shredded2, _) = shredCtx(q2)
    println("Shredded program: \n" + quote(shredded2) + "\n")

    val optShredded2 = optimize(shredded2)
    println("Shredded program optimized: \n" + quote(optShredded2) + "\n")

    val materializedProgram2 = materialize(optShredded2, opts)
    println("Materialized program (dict iterations): \n" + quote(materializedProgram2.program) + "\n")

    val unshredded2 = unshred(optShredded2, materializedProgram2.ctx)
    println("Unshredded program: \n" + quote(unshredded2) + "\n")

  }

  // this version of the query fails an assertion
  def matFailedAssertion(opts: Set[MaterializationOption]): Unit = {

    // third case throws a materialization error
    val q1 = genomic.EffectBySample2.program.asInstanceOf[Program]
    val (shredded, shreddedCtx) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program: \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

  }

  // not sure where this error is coming from
  def matTupleDictUnsupported1(opts: Set[MaterializationOption]): Unit = {
    val q1 = genomic.HybridBySample2.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  def matTupleDictUnsupported2(opts: Set[MaterializationOption]): Unit = {
    val q1 = genomic.HybridBySample2.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  def matTupleDictUnsupported3(opts: Set[MaterializationOption]): Unit = {
    val q1 = genomic.OccurGroupByCase0.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  def matTupleDictUnsupported4(opts: Set[MaterializationOption]): Unit = {
    val q1 = genomic.OccurGroupByCase.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  // an example that iterates through but doesn't fail
  def matTupleDictUnsupported5(opts: Set[MaterializationOption]): Unit = {
    val q1 = genomic.OccurGroupByCase1.program.asInstanceOf[Program]

    println("Program: \n" + quote(q1) + "\n")

    val (shredded, _) = shredCtx(q1)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

  def testLet(opts: Set[MaterializationOption]): Unit = {

    val lettest = genomic.LetTest0()
    val tbls = lettest.tbls

    // val imp = """if (t.impact = "HIGH") then 0.80 
    //               else if (t.impact = "MODERATE") then 0.50
    //               else if (t.impact = "LOW") then 0.30
    //               else 0.01"""

    // val query =
    //   s"""
    //       let cnvCases :=
    //         for s in samples union
    //           for c in copynumber union
    //             if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
    //             then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)}
    //     in
    //       (for o in occurrences union
    //         for t in o.transcript_consequences union
    //           for c in cnvCases union
    //             if (o.donorId = c.sid && t.gene_id = c.gene)
    //             then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := $imp * (c.cnum + 0.01))}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score})
    //   """

    // you can see this version in the file where the query is defined, 
    // but it just has multiple lets:
    // LetTest0 <= 
    //   let cnvCases := $cnvs
    //   in let initScores0 := $initScores 
    //   in $lettest
        
    val query = lettest.query

    val parser = Parser(tbls)
    // val program = parser.parse(query).get.asInstanceOf[Program]
    val bagexpr = parser.parse(query, parser.term).get.asInstanceOf[BagExpr]
    val program = Program(Assignment("LetTest0", bagexpr))

    println("Program: \n" + quote(program) + "\n")

    val (shredded, _) = shredCtx(program)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

  }

  def testDicts(program: Program, opts: Set[MaterializationOption]): Unit = {

    println("Program: \n" + quote(program) + "\n")

    val (shredded, _) = shredCtx(program)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")

  }

  def testTuple(opts: Set[MaterializationOption]): Unit = {
    val query1 = Tuple("y" -> tpch.Query1.query1.asInstanceOf[TupleAttributeExpr])
    val bag1 = BagProject(TupleVarRef("A", query1.tp), "y")
    val query2 =
      ForeachUnion(
        VarDef("x", bag1.tp.tp), bag1,
        Singleton(Tuple("c" -> PrimitiveProject(TupleVarRef("x", bag1.tp.tp), "c_name")))
    )

    val program = Program(Assignment("A", query1), Assignment("B", query2))

    println("Program: \n" + quote(program) + "\n")

    val (shredded, _) = shredCtx(program)
    println("Shredded program: \n" + quote(shredded) + "\n")

    val optShredded = optimize(shredded)
    println("Shredded program optimized: \n" + quote(optShredded) + "\n")

    val materializedProgram = materialize(optShredded, opts)
    println("Materialized program (if hoists + dict iteration): \n" + quote(materializedProgram.program) + "\n")

    val unshredded = unshred(optShredded, materializedProgram.ctx)
    println("Unshredded program: \n" + quote(unshredded) + "\n")
  }

//  val opts = Set.empty[MaterializationOption]
  val opts = Set[MaterializationOption](MOptEliminateDomains)
//  val opts = Set[MaterializationOption](MOptInlineLets)
//  val opts = Set[MaterializationOption](MOptEliminateDomains, MOptInlineLets)

//  // test multiple lets
//   testLet(opts)
//  // issue here when i do a nested aggregation, maintain
//  // input structure and then later associate them
//  testDicts(genomic.LetTest5.program.asInstanceOf[Program], opts)
//  testDicts(genomic.LetTest5.program2.asInstanceOf[Program], opts)
//  testDicts(genomic.LetTest5.f2fprogram.asInstanceOf[Program], opts)
//  testDicts(genomic.LetTest5.f2fprogram2.asInstanceOf[Program], opts)
//
//  runSequential(opts)
//  runSequential2(opts)
//  domainTest(opts)
//
//  dualConditionLabels(opts)
//  dualConditionLabels2(opts)
//  matFailedAssertion(opts)
//
//  matTupleDictUnsupported1(opts)
//  matTupleDictUnsupported2(opts)
//  matTupleDictUnsupported3(opts)
//  matTupleDictUnsupported4(opts)
//  // similar query that projects less attributes, and passes
//  matTupleDictUnsupported5(opts)
//
//
//  run(tpch.Query1.program.asInstanceOf[Program], opts)
//  run(tpch.Query2.program.asInstanceOf[Program], opts)
//  run(tpch.Query3.program.asInstanceOf[Program], opts)
//  run(tpch.Query4.program.asInstanceOf[Program], opts)
//  run(tpch.Query5.program.asInstanceOf[Program], opts)
//  run(tpch.Query6.program.asInstanceOf[Program], opts)
//  run(tpch.Query7.program.asInstanceOf[Program], opts)

//  testTuple(opts)
}
