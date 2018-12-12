package shredding.nrc

/**
  * NRCExprTest currently the main class for testing out
  * NRC queries
  */

object NRCExprTest extends App {

  object Example extends EmbedNRC with NRCTransforms {

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
                  Singleton(e.Project1), Singleton(e.Project2)))

      val q2 = q.Union(rr.Flatten)

      val q3 = rr.ForeachUnion(e1 =>
        e1.ForeachUnion(e2 =>
          Singleton(
            TupleStruct2(e2.Project1, e1))))

      val q4 = r.ForeachYield(e => e)

      println(Printer.quote(q1))
      println(Evaluator.eval(q1))
      //println(Printer.quote(q2))
      //println(Printer.quote(q3))
      //println(Printer.quote(q4))

//      println(q3.vars)
//      println(Evaluator.eval(q))
//      println(Evaluator.eval(rr))
//      println(Evaluator.eval(q2))
//      println(rr)
//      println(Evaluator.eval(q3))
//
      //println(Printer.quote(Shredder.shred(q)))
      //println(Printer.quote(Shredder.shred(q3)))

      val q5 = r.ForeachUnion(e1 =>
        Singleton(
          TupleStruct2(
            e1.Project1,
            r.ForeachUnion(e2 => Singleton(e2))
          )
        )
      )

      //println(Printer.quote(q5))
      //println(Evaluator.eval(q5))
      //println(Printer.quote(Shredder.shred(q5)))
    }
  }

  object Example2 extends EmbedNRC with NRCTransforms {

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

  object Example3 extends EmbedNRC with NRCTransforms {

    type Item = TTuple2[TBag[TTuple2[Int, TBag[TTuple2[Int, Int]]]], Int]

    def run(): Unit = {

      val r = Relation[Item]('R, 
        List[Item](
          (List((1, List((2, 3), (4,5))), (1, List((5, 6), (7,8)))), 8),
          (List((2, List((4, 5), (5,7)))), 6)
      ))

      val q = r.ForeachUnion(x => 
                x.Project1.ForeachUnion(y => 
                  Singleton(TupleStruct2(y.Project1, y.Project2.ForeachUnion(z => 
                    Singleton(TupleStruct1(z.Project1)))))))
      println(Printer.quote(q))
      println(Evaluator.eval(q))
      println(Evaluator.eval((Shredder.shred(q))))
    }
  }

  Example.run()
  //Example3.run()
  //Example2.run()
}
