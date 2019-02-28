package shredding.nrc

import scala.collection.mutable.Map
/**
  * NRCExprTest currently the main class for testing out
  * NRC queries
  */

object NRCExprTest extends App {

  object Example extends NRCTransforms {

    type Item = TTuple3[Int, String, Double]

    def run(): Unit = {

      val r = Relation[Item]('R,
        List[Item]((1, "Oxford", 4.33), (2, "Edinburgh", 1.33))
      )
      val rr = Relation[TBag[Item]]('RR,
        List(
          List[Item]((14, "Dummy", -4.2)),
          List[Item]((23, "Foo", 12), (45, "Bar", 2.4))
        ))

      val q = r.ForeachUnion(e => Singleton(e))

      val q1 = r.ForeachUnion(e => 
                IfThenElse(e.Project1.Equals(e.Project2), 
                  Singleton(TupleStruct1(e.Project1)), Singleton(TupleStruct1(Const(-1)))))

      // look into this
      val q2 = q.asInstanceOf[Expr[TBag[Item]]].Union(rr.Flatten)

      val q3 = rr.ForeachUnion(e1 =>
        e1.ForeachUnion(e2 =>
          Singleton(
            TupleStruct2(e2.Project1, e1))))

      //val q4 = r.ForeachYield(e => e)

      println(Printer.quote(q1))
      println(Evaluator.eval(q1))

      val q5 = r.ForeachUnion(e1 =>
        Singleton(
          TupleStruct2(
            e1.Project1,
            r.ForeachUnion(e2 => Singleton(e2))
          )
        )
      )

    }
  }

  object Example2 extends NRCTransforms {

    type Item = TTuple1[TBag[TTuple2[Int, TBag[TTuple1[Int]]]]]

    def run(): Unit = {

      val r = Relation[Item]('R, Nil)

      val q = r.ForeachUnion(x =>
        x.Project1.ForeachUnion(y =>
          Singleton(
            TupleStruct2(
              y.Project1,
              y.Project2.ForeachUnion(z =>
                Singleton(z.Project1))))))

      println(Printer.quote(q))
    }
  }

  object Example3 extends NRCTransforms {

    type Item = TTuple2[TBag[TTuple2[Int, TBag[TTuple2[Int, Int]]]], Int]

    def run(): Unit = {

      val r = Relation[Item]('R, 
        List[Item](
          (List((1, List((2, 3), (4,5))), (1, List((5, 6), (7,8)))), 8),
          (List((2, List((4, 5), (5,7)))), 6)
      ))

      type Item2 = TTuple2[Int, Int]
      val r2 = Relation[Item2]('R2,
        List[Item2]((1,2),(1,3),(1,4),(2,2),(2,3),(2,4))
      )

      val r3 = Relation[TTuple2[Int, Int]]('R3,
        List[TTuple2[Int, Int]]((1,1),(1,1),(1,1),(2,2),(2,2),(2,4))
      )

      val q = r.ForeachUnion(x => 
                x.Project1.ForeachUnion(y => 
                  Singleton(TupleStruct2(y.Project1, y.Project2.ForeachUnion(z => 
                    Singleton(TupleStruct1(z.Project1)))))))
      
      // this has the incorrect multiplicity issue
      val q2 = r2.ForeachUnion(x =>
                Singleton(TupleStruct2(x.Project1, r2.ForeachUnion(y => 
                  IfThenElse(y.Project1.Equals(x.Project1), 
                    Singleton(TupleStruct1(y.Project2)),
                    Singleton(TupleStruct1(Const(1))))))))

      val q3 = r2.ForeachUnion(x =>
                Singleton(TupleStruct2(x.Project1, r2.ForeachUnion(y => 
                  Singleton(TupleStruct1(x.Project2))))))
       
      }
    }

  object Example4 extends NRCTransforms {

    type Variant = TTuple3[String, Int, TBag[TTuple2[String, Int]]]
    type Clinical = TTuple2[String, Double]

    def run(): Unit = {

      val v = Relation[Variant]('V, 
        List[Variant](("1", 100, List(("one", 0), ("two", 1), ("three", 2), ("four", 0))),
          ("1", 101, List(("one", 1), ("two", 2), ("three", 2), ("four", 0))))) 
           
      val c = Relation[Clinical]('C,
        List[Clinical](("one", 1.0), ("two", 1.0), ("three", 0.0), ("four", 0.0)))
     
      // simple query to join clinical and genotype data on patient identifier
      val ac = v.ForeachUnion(x =>
                Singleton(TupleStruct3(x.Project1, x.Project2, c.ForeachUnion(y =>
                  Singleton(TupleStruct2(y.Project2, x.Project3.ForeachUnion(z =>
                    c.ForeachUnion(c2 => 
                      IfThenElse(And(c2.Project1.Equals(z.Project1), c2.Project2.Equals(y.Project2)),
                        Singleton(TupleStruct1(z.Project2)), EmptySet())))))))))

      val ac2 = v.ForeachUnion(x =>
                Singleton(TupleStruct3(x.Project1, x.Project2, x.Project3.ForeachUnion(y =>
                    c.ForeachUnion(z => 
                      IfThenElse(y.Project1.Equals(z.Project1),
                        Singleton(TupleStruct3(y.Project1, y.Project2, z.Project2)), EmptySet()))))))


      // even more simple query that does not join clinical data
      val t = v.ForeachUnion(x => // Q1
                Singleton(TupleStruct3(x._1, x._2, x._3.ForeachUnion(z => // Q2
                    Singleton(TupleStruct2(z._1, z._2))))))
 
      println("Input Query: \n"+Printer.quote(ac)+"\n")
      val queries = Shredder.generateShredQueries(ac)
      Printer.printQueries(queries)
      val outputs = Evaluator.evalQueries(queries)
      Printer.printOutputs(outputs)

    }
  }

  object Example5 extends NRCTransforms {
    
    def run(): Unit = {
      val S = Relation[Int]('S, List(1,1,1,1,1,4,4,4,2,3,3))
      val R = Relation[Tuple2[Int, TBag[Tuple2[Int,Int]]]]('R, 
                List((0, List((1,1), (2,2), (3,1))), 
                     (1, List((2,4), (2,2), (3,1))), 
                     (2, List((1,1), (2,1), (3,1)))))

      val q = R.ForeachUnion(x => 
                Singleton(TupleStruct2(x._1, x._2.ForeachUnion(w =>
                  Singleton(TupleStruct2(w._1, Mult(w._2, S)))))))
      println("Input Query: \n"+Printer.quote(q)+"\n")
      val queries = Shredder.generateShredQueries(q)
      Printer.printQueries(queries)
      val evals = Evaluator.evalQueries(queries)
      Printer.printOutputs(evals)
    }
  }

  object Example6 extends NRCTransforms {
    
    def run(): Unit = {
      val S = Relation[Int]('S, List(1,1,1,1,1,4,4,4,2,3,3))
      val R = Relation[Tuple2[Int, TBag[Tuple2[Int,Int]]]]('R, 
                List((0, List((1,1), (2,2), (3,1))), 
                     (1, List((2,4), (2,2), (3,1))), 
                     (2, List((1,1), (2,1), (3,1)))))

      val q = S.ForeachUnion(x => IfThenElse(Lt(x, 4), Singleton(x), EmptySet()))
      val cq = Calculus.translate(q)
      println(Printer.cquote(cq))
      val ncq = Unnester.unnest(cq)
      println(Printer.cquote(ncq))
      println("")

      val q11 = S.ForeachUnion(x => Singleton(TupleStruct1(x)))
      val cq11 = Calculus.translate(q11)
      println(Printer.cquote(cq11))
      //val ncq11 = Unnester.unnest(cq11)
      //println(Printer.cquote(ncq11))
      println("")
 
      val q1 = S.ForeachUnion(x => 
                S.ForeachUnion(y => 
                  IfThenElse(Eq(x, y), Singleton(TupleStruct2(x,y)), EmptySet())))
      val cq1 = Calculus.translate(q1)
      println(Printer.cquote(cq1))
      //val ncq1 = Unnester.unnest(cq1)
      //println(Printer.cquote(ncq1))
      println("")

      val q22 = R.ForeachUnion(x =>
                  x._2.ForeachUnion(y => Singleton(TupleStruct2(x._1, y._1))))
      val cq22 = Calculus.translate(q22)
      println(Printer.cquote(cq22))
      //val ncq22 = Unnester.unnest(cq22)
      //println(Printer.cquote(ncq22))
      println("")


      val q2 = S.ForeachUnion(x => Singleton(x))
      val cq2 = Calculus.translate(q2)
      println(Printer.cquote(cq2))
      println("")

      val q3 = S.ForeachUnion(x => IfThenElse(Lt(x,4), 
                                    S.ForeachUnion(y => Singleton(TupleStruct2(x,y))), EmptySet()))
      val cq3 = Calculus.translate(q3)
      println(Printer.cquote(cq3))
      //val ncq3 = Unnester.unnest(cq3)
      //println(Printer.cquote(ncq3))
      println("")

      val q41 = R.ForeachUnion(x => x._2.ForeachUnion(
                y => Singleton(TupleStruct2(x._1, y._1))))
      val cq41 = Calculus.translate(q41)
      println(Printer.cquote(cq41))
      //val ncq41 = Unnester.unnest(cq41)
      //println(Printer.cquote(ncq41))
      println("")

      val q4 = R.ForeachUnion(x => Singleton(TupleStruct2(x._1, x._2.ForeachUnion(
                y => Singleton(TupleStruct1(y._1))))))
      val cq4 = Calculus.translate(q4)
      println(Printer.cquote(cq4))
      //val ncq4 = Unnester.unnest2(cq4)
      //println(Printer.cquote(ncq4))
      println("")

      val q42 = R.ForeachUnion(x => x._2.ForeachUnion(
                  y => Singleton(TupleStruct2(y._1, y._2))))
      val cq42 = Calculus.translate(q42)
      println(Printer.cquote(cq42))
      //val ncq42 = Unnester.unnest2(cq42)
      //println(Printer.cquote(ncq42))
      println("")


      val q5 = Singleton(TupleStruct2(Const(1), Const(2))).ForeachUnion(x => Singleton(x._1))
      val cq5 = Calculus.translate(q5)
      println(Printer.cquote(cq5))
      println("")

      val q6 = EmptySet[Int]().ForeachUnion(x => Singleton(TupleStruct1(x)))
      val cq6 = Calculus.translate(q6)
      println(cq6)
      println(Printer.cquote(cq6))
      println("")

      val q7 = S.ForeachUnion(x => 
                Singleton(TupleStruct1(x))).ForeachUnion(s => R.ForeachUnion(x => 
                  x._2.ForeachUnion( y => Singleton(TupleStruct2(s,y)._1))))
      val cq7 = Calculus.translate(q7)
      println(cq7)
      println(Printer.cquote(cq7))
      //val ncq7 = Unnester.unnest(cq7)
      //println(Printer.cquote(ncq7))
      println("")

      val q8 = S.ForeachUnion(x => EmptySet[Int]().ForeachUnion(y => Singleton(TupleStruct1(y))))
      val cq8 = Calculus.translate(q8)
      println(cq8)
      println(Printer.cquote(cq8))
      println("")
    }
  }

  Example6.run()
}
