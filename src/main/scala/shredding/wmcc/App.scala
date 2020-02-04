package shredding.wmcc

import shredding.core._
import shredding.examples.genomic._
import shredding.examples.tpch._
import shredding.runtime.{Context, Evaluator}
import shredding.nrc.{MaterializeNRC, ShredNRC, Printer => NRCPrinter}

object App {
  
  val nrceval = new Evaluator{}
  val nrcprinter = new NRCPrinter with ShredNRC with MaterializeNRC{}
  val ctx = new Context()

  val translator = new NRCTranslator{}
  val normalizer = new Finalizer(new BaseNormalizer{})
  val runner = new PipelineRunner{}
  val anfBase = new BaseANF {}
  val anfer = new Finalizer(anfBase)

  def getCCParams(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) {(a, f) =>
      f.setAccessible(true)
    a + (f.getName -> f.get(cc))
  }

  def main(args: Array[String]){
    println(runner.makeBag)
    //runTPCH()  
    //runGenomic()
    //runPathway()
  }

  def runPathway(){
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)
    
    def populate = {
      eval.ctx.clear
      eval.ctx("cases") = GenomicRelations.cases
      eval.ctx("variants") = GenomicRelations.variants
      eval.ctx("clinical") = GenomicRelations.clinical
      eval.ctx("cases__F") = GenomicRelations.cases__F
      eval.ctx("cases__D") = GenomicRelations.cases__D
      eval.ctx("variants__F") = GenomicRelations.variants__F
      eval.ctx("variants__D") = GenomicRelations.variants__D
      eval.ctx("clinical__F") = GenomicRelations.clinical__F
      eval.ctx("clinical__D") = GenomicRelations.clinical__D
    }

    println(" ----------------------- Query 0: simple  ----------------------- ")
    //populate
    val q0 = normalizer.finalize(
              translator.translate(
                PathwayTests.q0.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]

    println(s"${runner.quote(PathwayTests.q0.asInstanceOf[runner.Expr])}\n")

    println(Printer.quote(q0))
    //println(s"${evaluator.finalize(q1)}\n")
    
    val p0 = Unnester.unnest(q0)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p0))
    //println(s"${evaluator.finalize(p1)}\n")

    val anfedq0 = anfer.finalize(p0)
    val anfExp0 = anfBase.anf(anfedq0.asInstanceOf[anfBase.Rep])
    //println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))

    val sq0 = normalizer.finalize(
                runner.shredPipeline(
                  PathwayTests.q0.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]
    
    println(Printer.quote(sq0.asInstanceOf[CExpr]))
    //println(s"${evaluator.finalize(sq1)}\n") **/

    println(" ----------------------- Query 1: simple mutation burden ----------------------- ")
    //populate
    val q1 = normalizer.finalize(
              translator.translate(
                PathwayTests.q1.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]

    println(s"${runner.quote(PathwayTests.q1.asInstanceOf[runner.Expr])}\n")

    println(Printer.quote(q1))
    //println(s"${evaluator.finalize(q1)}\n")
    
    val p1 = Unnester.unnest(q1)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p1))
    //println(s"${evaluator.finalize(p1)}\n")

    val anfedq1 = anfer.finalize(p1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    //println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))

    val sq1 = normalizer.finalize(
                runner.shredPipeline(
                  PathwayTests.q1.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]
    
    println(Printer.quote(sq1.asInstanceOf[CExpr]))
    //println(s"${evaluator.finalize(sq1)}\n") **/

  }

  def runGenomic(){
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)
    
    def populate = {
      eval.ctx.clear
      eval.ctx("cases") = GenomicRelations.cases
      eval.ctx("variants") = GenomicRelations.variants
      eval.ctx("clinical") = GenomicRelations.clinical
      eval.ctx("cases__F") = GenomicRelations.cases__F
      eval.ctx("cases__D") = GenomicRelations.cases__D
      eval.ctx("variants__F") = GenomicRelations.variants__F
      eval.ctx("variants__D") = GenomicRelations.variants__D
      eval.ctx("clinical__F") = GenomicRelations.clinical__F
      eval.ctx("clinical__D") = GenomicRelations.clinical__D
    }

    println(" ----------------------- Query 1: alternate allele count ----------------------- ")
    populate
    val q1 = normalizer.finalize(
              translator.translate(
                GenomicTests.q1.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]

    println(s"${runner.quote(GenomicTests.q1.asInstanceOf[runner.Expr])}\n")

    println(Printer.quote(q1))
    println(s"${evaluator.finalize(q1)}\n")
    
    val p1 = Unnester.unnest(q1)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p1))
    println(s"${evaluator.finalize(p1)}\n")

    val anfedq1 = anfer.finalize(p1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))

    val sq1 = normalizer.finalize(
                runner.shredPipeline(
                  GenomicTests.q1.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]
    
    println(Printer.quote(sq1.asInstanceOf[CExpr]))
    println(s"${evaluator.finalize(sq1)}\n") 

    println(" ----------------------- Query 2: count the number of genotypes ----------------------- ")
    populate
    val q2 = normalizer.finalize(
              translator.translate(
                GenomicTests.q2.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]

    println(s"${runner.quote(GenomicTests.q2.asInstanceOf[runner.Expr])}\n")

    println(Printer.quote(q2))
    println(s"${evaluator.finalize(q2)}\n")
    
    val p2 = Unnester.unnest(q2)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p2))
    println(s"${evaluator.finalize(p2)}\n")

    val anfedq2 = anfer.finalize(p2)
    val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp2.asInstanceOf[CExpr]))

    val sq2 = normalizer.finalize(
                runner.shredPipeline(
                  GenomicTests.q2.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]
                      
    println(Printer.quote(sq2.asInstanceOf[CExpr]))
    println(s"${evaluator.finalize(sq2)}\n") 

    println(" ----------------------- Query 5: allele counts ----------------------- ")
    
    populate
    val q5 = normalizer.finalize(
              translator.translate(
                GenomicTests.q5.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]

    println(s"${runner.quote(GenomicTests.q5.asInstanceOf[runner.Expr])}\n")

    println(Printer.quote(q5))
    println(s"${evaluator.finalize(q5)}\n")
    
    val p5 = Unnester.unnest(q5)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p5))
    println(s"${evaluator.finalize(p5)}\n")

    val anfedq5 = anfer.finalize(p5)
    val anfExp5 = anfBase.anf(anfedq5.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp5.asInstanceOf[CExpr]))

    val sq5 = normalizer.finalize(
                runner.shredPipeline(
                  GenomicTests.q5.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]).asInstanceOf[CExpr]
                      
    println(Printer.quote(sq5.asInstanceOf[CExpr]))
    println(s"${evaluator.finalize(sq5)}\n") 

    println(" ----------------------- Query 6: Setup Genotypes for Mixed Linear Model ------------------- ")
    
    populate
    val unq6 = translator.translate(
                GenomicTests.q6.asInstanceOf[translator.Expr]).asInstanceOf[CExpr]

    val q6 = normalizer.finalize(unq6).asInstanceOf[CExpr]

    println(s"${runner.quote(GenomicTests.q6.asInstanceOf[runner.Expr])}\n")

    println(s"${Printer.quote(unq6)}\n")
    println(Printer.quote(q6))
    println(s"${evaluator.finalize(q6)}\n")
    
    val p6 = Unnester.unnest(q6)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(Printer.quote(p6))
    println(s"${evaluator.finalize(p6)}\n")

    val anfedq6 = anfer.finalize(p6)
    val anfExp6 = anfBase.anf(anfedq6.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp6.asInstanceOf[CExpr]))

    val unsq6 = runner.shredPipeline(
                  GenomicTests.q6.asInstanceOf[runner.Expr]).asInstanceOf[CExpr]

    val sq6 = normalizer.finalize(unsq6).asInstanceOf[CExpr]
                      
    println(Printer.quote(unsq6.asInstanceOf[CExpr]))
    println(Printer.quote(sq6.asInstanceOf[CExpr]))
    println(s"${evaluator.finalize(sq6)}\n") 

 
  }

  def runTPCH(){  
    val eval = new BaseScalaInterp{}
    val evaluator = new Finalizer(eval)

    /**val q8 = translator.translate(NestedTests.q1.asInstanceOf[translator.Expr])
    val nq8 = normalizer.finalize(q8).asInstanceOf[CExpr]
    println(Printer.quote(nq8.asInstanceOf[CExpr]))
    eval.ctx("R") = NestedRelations.format1a
    println(evaluator.finalize(nq8.asInstanceOf[CExpr]))
    val p8 = Unnester.unnest(nq8)((Nil, Nil, None)).asInstanceOf[CExpr]
    println(evaluator.finalize(p8))**/

  
    /**val q1 = translator.translate(NestedTests.q10.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    eval.ctx("R") = NestedRelations.format4a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    println(evaluator.finalize(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))

    val sq1 = runner.shredPipeline(NestedTests.q10.asInstanceOf[runner.Expr])
    val snormq1 = normalizer.finalize(sq1).asInstanceOf[CExpr]
    println(Printer.quote(snormq1.asInstanceOf[CExpr]))
    eval.ctx.clear
    eval.ctx("R__F") = 1
    eval.ctx("R__D") = NestedRelations.sformat4a
    println(evaluator.finalize(snormq1.asInstanceOf[CExpr]))
    val splan1 = Unnester.unnest(snormq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(splan1))
    println(evaluator.finalize(splan1))
    anfBase.reset
    val sanfedq1 = anfer.finalize(splan1)
    val sanfExp1 = anfBase.anf(sanfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(sanfExp1.asInstanceOf[CExpr]))**/

    /**val q1 = translator.translate(FlatTests.q1.asInstanceOf[translator.Expr])
    val normq1 = normalizer.finalize(q1).asInstanceOf[CExpr]
    println(Printer.quote(normq1.asInstanceOf[CExpr]))
    eval.ctx("R") = FlatRelations.format1a
    println(evaluator.finalize(normq1.asInstanceOf[CExpr]))
    val plan1 = Unnester.unnest(normq1)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan1))
    println(evaluator.finalize(plan1))
    val anfedq1 = anfer.finalize(plan1)
    val anfExp1 = anfBase.anf(anfedq1.asInstanceOf[anfBase.Rep])
    println(evaluator.finalize(anfExp1.asInstanceOf[CExpr]))**/

    println("\n---------------------------------------- QUERY 1 JOIN ORDER -----------------------\n")
    val qa = nrceval.Assignment("Query4", TPCHQueries.query1a.asInstanceOf[nrceval.BagExpr])
    println(nrcprinter.quote(qa.asInstanceOf[nrcprinter.Expr]))

    ctx.add(VarDef("C", TPCHSchema.customertype), TPCHLoader.loadCustomer[Customer].toList.map(getCCParams(_))) 
    ctx.add(VarDef("O", TPCHSchema.orderstype), TPCHLoader.loadOrders[Orders].toList.map(getCCParams(_)))
    ctx.add(VarDef("L", TPCHSchema.lineittype), TPCHLoader.loadLineitem[Lineitem].toList.map(getCCParams(_)))
    ctx.add(VarDef("P", TPCHSchema.parttype), TPCHLoader.loadPart[Part].toList.map(getCCParams(_)))
    println("\nNRC EVALUATED:\n")
    println(nrceval.eval(qa, ctx))
  
    val q2a = translator.translate(
      translator.Assignment("Query4", TPCHQueries.query1a.asInstanceOf[translator.BagExpr]))
    
   
    val normq2a = normalizer.finalize(q2a).asInstanceOf[CExpr]
    println(s"\n${Printer.quote(normq2a)}\n")
    eval.ctx("C") = TPCHLoader.loadCustomer[Customer].toList 
    eval.ctx("O") = TPCHLoader.loadOrders[Orders].toList 
    eval.ctx("L") = TPCHLoader.loadLineitem[Lineitem].toList 
    eval.ctx("P") = TPCHLoader.loadPart[Part].toList 

    println("\nNORMALIZED CALCULUS EVALUATED:\n")
    println(evaluator.finalize(normq2a))
    
    val plan2a = Unnester.unnest(normq2a)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan\n")
    println(Printer.quote(plan2a))
    println("\nEVALUATED PLAN:\n")
    println(evaluator.finalize(plan2a))
    anfBase.reset
    val anfedq2a = anfer.finalize(plan2a)
    val anfExp2a = anfBase.anf(anfedq2a.asInstanceOf[anfBase.Rep])
    //println("this is the evaluated anf plan")
    //println(evaluator.finalize(anfExp2.asInstanceOf[CExpr]))

    println("\n---------------------------------------- QUERY 1 -----------------------\n")
    val q = nrceval.Assignment("Query4", TPCHQueries.query1.asInstanceOf[nrceval.BagExpr])
 
    ctx.add(VarDef("C", TPCHSchema.customertype), TPCHLoader.loadCustomer[Customer].toList.map(getCCParams(_))) 
    ctx.add(VarDef("O", TPCHSchema.orderstype), TPCHLoader.loadOrders[Orders].toList.map(getCCParams(_)))
    ctx.add(VarDef("L", TPCHSchema.lineittype), TPCHLoader.loadLineitem[Lineitem].toList.map(getCCParams(_)))
    ctx.add(VarDef("P", TPCHSchema.parttype), TPCHLoader.loadPart[Part].toList.map(getCCParams(_)))
    println("\nNRC EVALUATED:\n")
    println(nrceval.eval(q, ctx))
  
    val q2 = translator.translate(
      translator.Assignment("Query4", TPCHQueries.query1.asInstanceOf[translator.BagExpr]))
    
   
    val normq2 = normalizer.finalize(q2).asInstanceOf[CExpr]
    println(s"\n${Printer.quote(normq2)}\n")
    eval.ctx("C") = TPCHLoader.loadCustomer[Customer].toList 
    eval.ctx("O") = TPCHLoader.loadOrders[Orders].toList 
    eval.ctx("L") = TPCHLoader.loadLineitem[Lineitem].toList 
    eval.ctx("P") = TPCHLoader.loadPart[Part].toList 
    //eval.ctx("PS") = TPCHLoader.loadPartSupp[PartSupp].toList 
    //eval.ctx("S") = TPCHLoader.loadSupplier[Supplier].toList 
    println("\nNORMALIZED CALCULUS EVALUATED:\n")
    println(evaluator.finalize(normq2))
    
    val plan2 = Unnester.unnest(normq2)(Nil, Nil, None)
    println("\nPlan\n")
    println(Printer.quote(plan2))
    println("\nEVALUATED PLAN:\n")
    println(evaluator.finalize(plan2))
    anfBase.reset
    val anfedq2 = anfer.finalize(plan2)
    val anfExp2 = anfBase.anf(anfedq2.asInstanceOf[anfBase.Rep])
    //println("this is the evaluated anf plan")
    //println(evaluator.finalize(anfExp2.asInstanceOf[CExpr]))

    println("\n---------------------------------------- QUERY 4 -----------------------\n")

    val q0a = TPCHQueries.query4a.asInstanceOf[nrceval.Expr]
    println("\nNRC EVALUTED:\n")
    println(nrceval.eval(q0a, ctx))
    val q5a = translator.translate(TPCHQueries.query4a.asInstanceOf[translator.Expr])
    val normq5a = normalizer.finalize(q5a).asInstanceOf[CExpr]
    println("\nNormalized Calculus:\n")
    println(Printer.quote(normq5a))
    println("\nNORMALIZED CALC EVALUATED")
    println(evaluator.finalize(normq5a))
    
    val plan5a = Unnester.unnest(normq5a)(Nil, Nil, None)
    println("\nPlan:\n")
    println(Printer.quote(plan5a))
    println("\nPLAN EVALUTED:\n")
    println(evaluator.finalize(plan5a))
    anfBase.reset
    val anfedq5a = anfer.finalize(plan5a)
    val anfExp5a = anfBase.anf(anfedq5a.asInstanceOf[anfBase.Rep])
    //println(evaluator.finalize(anfExp5.asInstanceOf[CExpr]))


    println("\n---------------------------------------- QUERY 4, No Duplicates -----------------------\n")

    val q0 = TPCHQueries.query4.asInstanceOf[nrceval.Expr]
    println("\nNRC EVALUTED:\n")
    println(nrceval.eval(q0, ctx))
    val q5 = translator.translate(TPCHQueries.query4.asInstanceOf[translator.Expr])
    val normq5 = normalizer.finalize(q5).asInstanceOf[CExpr]
    println("\nNormalized Calculus:\n")
    println(Printer.quote(normq5))
    println("\nNORMALIZED CALC EVALUATED")
    println(evaluator.finalize(normq5))
    
    val plan5 = Unnester.unnest(normq5)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan:\n")
    println(Printer.quote(plan5))
    println("\nPLAN EVALUTED:\n")
    println(evaluator.finalize(plan5))
    anfBase.reset
    val anfedq5 = anfer.finalize(plan5)
    val anfExp5 = anfBase.anf(anfedq5.asInstanceOf[anfBase.Rep])
    //println(evaluator.finalize(anfExp5.asInstanceOf[CExpr]))

    //val sq2 = runner.shredPipeline(TPCHQueries.query1.asInstanceOf[runner.Expr])
    //val snormq2 = normalizer.finalize(sq2).asInstanceOf[CExpr]
    println("")
    //println(Printer.quote(snormq2))
    eval.ctx.clear
    println("\n-------------------- Query 4, Group By -----------------------------\n")

    val q0b = TPCHQueries.query4b.asInstanceOf[nrceval.Expr]
    println(nrcprinter.quote(q0b.asInstanceOf[nrcprinter.Expr]))
    //println("\nNRC EVALUTED:\n")
    //println(nrceval.eval(q0b, ctx))
    println("\ncalculus: \n")
    val q4b = translator.translate(TPCHQueries.query4b.asInstanceOf[translator.Expr])
    println(Printer.quote(q4b))
    val normq4b = normalizer.finalize(q4b).asInstanceOf[CExpr]
    println("\nNormalized Calculus:\n")
    println(Printer.quote(normq4b))
    //println("\nNORMALIZED CALC EVALUATED")
    //println(evaluator.finalize(normq5a))
    
    val plan4b = Unnester.unnest(normq4b)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nPlan:\n")
    println(Printer.quote(plan4b))
    //println("\nPLAN EVALUTED:\n")
    //println(evaluator.finalize(plan5a))
    anfBase.reset
    val anfedq4b = anfer.finalize(plan4b)
    val anfExp4b = anfBase.anf(anfedq4b.asInstanceOf[anfBase.Rep])
    //println(evaluator.finalize(anfExp5.asInstanceOf[CExpr]))



    /**eval.ctx("C__F") = 1
    eval.ctx("C__D") = (List((1, TPCHLoader.loadCustomer[Customer].toList)), ())
    eval.ctx("O__F") = 2
    eval.ctx("O__D") = (List((2, TPCHLoader.loadOrders[Orders].toList)), ())
    eval.ctx("L__F") = 3
    eval.ctx("L__D") = (List((3, TPCHLoader.loadLineitem[Lineitem].toList)), ())
    eval.ctx("P__F") = 4
    eval.ctx("P__D") = (List((4, TPCHLoader.loadPart[Part].toList)), ())
    eval.ctx("PS__F") = 5
    eval.ctx("PS__D") = (List((5, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
    eval.ctx("S__F") = 6
    eval.ctx("S__D") = (List((6, TPCHLoader.loadSupplier[Supplier].toList)), ())
    
    println(evaluator.finalize(snormq2))
    
    val splan2 = Unnester.unnest(snormq2)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nShredPlan")
    println(Printer.quote(splan2))
    println(evaluator.finalize(splan2))
    anfBase.reset
    val anfedqs2 = anfer.finalize(splan2)
    val anfExps2 = anfBase.anf(anfedqs2.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExps2))
    println(evaluator.finalize(anfExps2.asInstanceOf[CExpr]))

    eval.ctx("Query5__F") = eval.ctx("M_ctx1").asInstanceOf[List[_]].head.asInstanceOf[RecordValue].map("lbl")
    println("\n query 5 flat")
    println(eval.ctx("Query5__F"))
    def makeInput(k: Any): List[(Any, List[Any])] = k match {
      case l:List[_] => l.asInstanceOf[List[RecordValue]].map{ case rv => (rv.map("k"), rv.map("v").asInstanceOf[List[Any]]) }
      case _ => ???
    }
    eval.ctx("Query5__D") = (makeInput(eval.ctx("M_flat1")), (RecordValue("customers" -> (makeInput(eval.ctx("M_flat3")), ()), "suppliers" -> (makeInput(eval.ctx("M_flat2")), ()))))
    println("\n query 5 dict")
    println(eval.ctx("Query5__D"))

    val sq5 = runner.shredPipeline(TPCHQueries.query5.asInstanceOf[runner.Expr])
    val snormq5 = normalizer.finalize(sq5).asInstanceOf[CExpr]
    println("")
    println(Printer.quote(snormq5))
    println(evaluator.finalize(snormq5))

    val splan5 = Unnester.unnest(snormq5)(Nil, Nil, None).asInstanceOf[CExpr]
    println("\nShredPlan")
    println(Printer.quote(splan5))
    println(evaluator.finalize(splan5))
    anfBase.reset
    val anfedqs5 = anfer.finalize(splan5)
    val anfExps5 = anfBase.anf(anfedqs5.asInstanceOf[anfBase.Rep])
    println(Printer.quote(anfExps5))
    println(evaluator.finalize(anfExps5.asInstanceOf[CExpr]))**/

    /**val q3 = {    
      import translator._
      val tuple_x = TupleType("a" -> IntType, "c" -> IntType)
      val tuple_y = TupleType("a" -> IntType, "c" -> IntType)
      val relationR = BagVarRef(VarDef("R", BagType(tuple_x)))

      val xdef = VarDef("x", tuple_x)
      val ydef = VarDef("y", tuple_y)
      val xref = TupleVarRef(xdef)

      val yref = TupleVarRef(ydef)
      translate(Tuple("a" -> xref("c"), "b" -> ForeachUnion(ydef, relationR,
        IfThenElse(
          Cmp(OpEq, yref("c"), xref("c")),
          Singleton(Tuple("b'" -> yref("a"))), 
          Singleton(Tuple("b'" -> Const(-1, IntType)))).asInstanceOf[BagExpr]
      )))
    }
    val normq3 = normalizer.finalize(q3)
    println(Printer.quote(normq3.asInstanceOf[CExpr]))
    eval.ctx("R") = List(
      RecordValue("a" -> 1, "c" -> 11),
      RecordValue("a" -> 2, "c" -> 22),
      RecordValue("a" -> 3, "c" -> 33),
      RecordValue("a" -> 4, "c" -> 44)
    )
    eval.ctx("x") =  RecordValue("a" -> 4, "c" -> 44)
    println(evaluator.finalize(normq3.asInstanceOf[CExpr]))
    **/

    // bug in this one
    /**val q4 = translator.translate(NestedTests.q6.asInstanceOf[translator.Expr])
    val sq4 = runner.shredPipeline(NestedTests.q6.asInstanceOf[runner.Expr])
    val normq4 = normalizer.finalize(q4).asInstanceOf[CExpr]
    val snormq4 = normalizer.finalize(sq4).asInstanceOf[CExpr]
    println(Printer.quote(normq4.asInstanceOf[CExpr]))
    eval.ctx.clear
    eval.ctx("R") = NestedRelations.format2a
    println(evaluator.finalize(normq4.asInstanceOf[CExpr]))
    val plan4 = Unnester.unnest(normq4)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(plan4))
    println(evaluator.finalize(plan4))

    eval.ctx("R__F") = 1
    eval.ctx("R__D") = NestedRelations.format2Dd
    println(Printer.quote(snormq4.asInstanceOf[CExpr]))
    val splan4 = Unnester.unnest(snormq4)(Nil, Nil, None).asInstanceOf[CExpr]
    println(Printer.quote(splan4))
    println(evaluator.finalize(splan4))**/
  }

}
