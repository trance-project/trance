package shredding.nrc

import org.scalatest.FunSuite

class UnnesterTest extends FunSuite{

    // get the new variable before it is created
    def vari(x: String) = x+(VarCnt.currId+1)

    /**
      * Unit test for the example on page 486 of F & M 
      * Starts with input NRC, translates to comprehension calculus
      * and unnests into algebraic form
      */
    test("Unnester.unnest.F&M.BasicExample"){

      val ctype = TupleType("name" -> StringType)
      val etype = TupleType("name" -> StringType, "children" -> BagType(ctype))
      val e = VarDef("e", etype)
      val c = VarDef("c", ctype)
      val employees = Relation("Employees", PhysicalBag(etype,
                    Tuple("name" -> Const("one", StringType), "children" -> PhysicalBag(ctype,
                      Tuple("name" -> Const("child1", StringType)), Tuple("name" -> Const("child2", StringType)))),
                    Tuple("name" -> Const("two", StringType), "children" -> PhysicalBag(ctype,
                      Tuple("name" -> Const("child3", StringType)), Tuple("name" -> Const("child4", StringType)))),
                    Tuple("name" -> Const("three", StringType), "children" -> PhysicalBag(ctype,
                      Tuple("name" -> Const("child1", StringType)), Tuple("name" -> Const("child3", StringType))))
                  ))
      // For e in Employees Union
      //   For c in e.children Union 
      //     sng( E := e.name, C := c.name ) 
      val q = ForeachUnion(e, employees, 
                ForeachUnion(c, VarRef(e, "children").asInstanceOf[BagExpr],
                  Singleton(Tuple("E" -> VarRef(e, "name"), "C" -> VarRef(c, "name")))))
      
      // { ( E = e.name, C = c.name ) | e <- Employees, c <- e.children }
      val cq = BagComp(Tup("E" -> Var(Var(e), "name"), "C" -> Var(Var(c), "name")), 
              List(Generator(e, InputR(employees.n, employees.b)), 
                   Generator(c, Var(Var(e), "children").asInstanceOf[BagCalc])))

      assert(Translator.translate(q) == cq)
      // Reduce U / ( E = e.name, C = c.name )
      //  | 
      //  c
      //  |
      // Unnest e.children
      //  |
      //  e
      //  |
      // Employees
      val nopreds = List[PrimitiveCalc]()
      val ncq = Unnester.unnest(cq) 
      assert(ncq == Term(
                    Reduce(Tup("E" -> Var(Var(e), "name"), "C" -> Var(Var(c), "name")), List(c,e), nopreds),
                      Term(Unnest(List(c, e), Var(Var(e), "children").asInstanceOf[BagCalc], nopreds), 
                        Select(InputR(employees.n, employees.b), e, nopreds))))
    
      
    }

    /**
      * Query A unnesting example from F & M (7.1)
      */
    test("Unnester.unnest.F&M.QueryA"){

      val dtype = TupleType("dno" -> IntType)
      val etype = TupleType("dno" -> IntType)
      val e = VarDef("e", etype)
      val d = VarDef("d", dtype)
      val employees = Relation("Employees", PhysicalBag(etype,
                    Tuple("dno" -> Const("1", IntType)), Tuple("dno" -> Const("2", IntType)), 
                    Tuple("dno" -> Const("3", IntType)), Tuple("dno" -> Const("4", IntType))))
      val departments = Relation("Departments", PhysicalBag(dtype,
                    Tuple("dno" -> Const("1", IntType)), Tuple("dno" -> Const("2", IntType)), 
                    Tuple("dno" -> Const("3", IntType)), Tuple("dno" -> Const("4", IntType))))

      // For d in Departments Union 
      //  sng( D := d.dno, E := For e in Employees Union 
      //                          If (e.dno = d.dno)
      //                          Then sng(e)
      val q = ForeachUnion(d, departments, 
                Singleton(Tuple("D" -> VarRef(d, "dno"), "E" -> ForeachUnion(e, employees, 
                                                      IfThenElse(Cond(OpEq, VarRef(e, "dno"), VarRef(d, "dno")), 
                                                                 Singleton(VarRef(e).asInstanceOf[TupleExpr]))))))

      // { ( D = d.dno, E = { e | e <- Employees, e.dno = d.dno } | d <- Departments }
      val cq = BagComp(Tup("D" -> Var(Var(d), "dno"), "E" -> BagComp(Var(e).asInstanceOf[TupleCalc], 
                                                  List(Generator(e, InputR(employees.n, employees.b)),
                                                       Conditional(OpEq, Var(Var(e), "dno"), Var(Var(d), "dno"))))),
                List(Generator(d, InputR(departments.n, departments.b))))
     
      assert(Translator.translate(q) == cq)

      // Reduce U / (D = d.dno, E = m ) 
      //          |
      //          m
      //          |
      //       Nest U / e, d
      //          |
      // Outer-join e.dno = d.dno
      //      /         \
      //     d           e
      //    /             \
      // Departments    Employees
      val v = vari("v")
      val nq = Unnester.unnest(cq)
      val nopreds = List[PrimitiveCalc]()
      assert(nq == Term(Reduce(Tup("D" -> Var(Var(d), "dno"), "E" -> BagVar(v, None, BagType(etype))), 
                          List(VarDef("v", BagType(etype), v.replace("v", "").toInt), d), nopreds),                    
                      Term(Nest(Var(e), List(e, d), List(d), nopreds, List(e)),
                        Term(Term(OuterJoin(List(e,d), List(Conditional(OpEq, Var(Var(e), "dno"), Var(Var(d), "dno")))), 
                          Select(InputR(employees.n, employees.b), e, nopreds)), 
                            Select(InputR(departments.n, departments.b), d, nopreds)))))
    }

    /**
      * QueryC from F & M 
      * 
      * U { ( E := e.name, M := + { 1 | c <- e.children, 
      *                                 & { c.age > d.age | d <- e.manager.children } }) 
      *   | e <- Employees } 
      *
      * Source NRC: use TotalMult and 0's and 1's to replicate AND accumulator
      * For e in Employees union
      *   sng( E := e.name, M := TotalMult( For c in e.children union 
      *                                       if TotalMult( For d <- e.manager.children union 
      *                                                       if c.age <= d.age then sng(c)) == 0 
      *                                       then sng(c) )
      *
      * WMCC:
      * { ( E := e27.name, M :=  + { 1 |  c28 <- e27.children ,   
      *                                         + { 1 |  m30 <- e27.manager ,  
      *                                                  d29 <- m30.children ,  d29.age >= c28.age  } = 0  } ) 
      *      |  e27 <- Employees  }
      */
    test("Unnester.unnest.F&M.QueryC"){
      val ctype = TupleType("name" -> StringType, "age" -> IntType)
      val dtype = TupleType("name" -> StringType, "children" -> BagType(ctype), "age" -> IntType)
      val etype = TupleType("name" -> StringType, "children" -> BagType(ctype), "manager" -> BagType(dtype))
      val e = VarDef("e", etype)
      val c = VarDef("c", ctype)
      val d = VarDef("d", ctype)
      val m = VarDef("m", dtype)
      val employees = Relation("Employees", PhysicalBag(etype,
                      Tuple("name" -> Const("one", StringType), 
                            "children" -> PhysicalBag(ctype,
                              Tuple("name" -> Const("child1", StringType), "age" -> Const("1", IntType)), 
                              Tuple("name" -> Const("child2", StringType), "age" -> Const("2", IntType))),
                            "manager" -> PhysicalBag(dtype, 
                              Tuple("name" -> Const("mone", StringType), "age" -> Const("40", IntType), 
                                    "children" -> PhysicalBag(ctype,
                                      Tuple("name" -> Const("child3", StringType), "age" -> Const("10", IntType)), 
                                      Tuple("name" -> Const("child4", StringType), "age" -> Const("12", IntType))))))))

       // Source NRC
       val q = ForeachUnion(e, employees, 
                Singleton(Tuple("E" -> VarRef(e, "name"), 
                  "M" -> TotalMult(ForeachUnion(c, VarRef(e, "children").asInstanceOf[BagExpr], 
                    IfThenElse(Cond(OpEq, TotalMult(ForeachUnion(m, VarRef(e, "manager").asInstanceOf[BagExpr],
                      ForeachUnion(d, VarRef(m, "children").asInstanceOf[BagExpr], 
                        IfThenElse(Cond(OpGe, VarRef(d, "age"), VarRef(c, "age")),
                          Singleton(VarRef(c).asInstanceOf[TupleExpr]))))), Const("0", IntType)), 
                      Singleton(VarRef(c).asInstanceOf[TupleExpr])))))))
       
       // WMCC (comprehension calculus)
       val cq = BagComp(Tup("E" -> Var(Var(e), "name"), "M" -> CountComp(Constant("1", IntType), 
                  List(Generator(c, Var(Var(e), "children").asInstanceOf[BagCalc]), 
                       Conditional(OpEq, CountComp(Constant("1", IntType), 
                                          List(Generator(m, Var(Var(e), "manager").asInstanceOf[BagCalc]), 
                                               Generator(d, Var(Var(m), "children").asInstanceOf[BagCalc]),
                                               Conditional(OpGe, Var(Var(d), "age"), Var(Var(c), "age")))), Constant("0", IntType))))), 
                List(Generator(e, InputR(employees.n, employees.b))))

       // validate source to wmcc translation
       assert(Translator.translate(q) == cq)
    
       val ncq = Unnester.unnest(cq)
    }

}
