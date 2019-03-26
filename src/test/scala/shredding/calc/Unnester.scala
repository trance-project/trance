package shredding.calc

import org.scalatest.FunSuite
import shredding.core._
import shredding.nrc2._

class UnnesterTest extends FunSuite with CalcTranslator with CalcImplicits with NRCTranslator with NRC{

    def print(e: CompCalc) = println(calc.quote(e.asInstanceOf[calc.CompCalc]))    
    
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
      val employees = Relation("Employees", List(
                        Map("name" -> "one", "children" -> List(Map("name" -> "child1"), Map("name" -> "child2"))),
                        Map("name" -> "two", "children" -> List(Map("name" -> "child3"), Map("name" -> "child4"))),
                        Map("name" -> "three", "children" -> List(Map("name" -> "child1"), Map("name" -> "child3"))),
                    ), BagType(etype))

      // For e in Employees Union
      //   For c in e.children Union 
      //     sng( E := e.name, C := c.name ) 
      val q = ForeachUnion(e, employees, 
                ForeachUnion(c, Project(TupleVarRef(e), "children").asInstanceOf[BagExpr],
                  Singleton(Tuple("E" -> Project(TupleVarRef(e), "name"), "C" -> Project(TupleVarRef(c), "name")))))
      val tq = Translator.translate(q)
       // { ( E = e.name, C = c.name ) | e <- Employees, c <- e.children }
      val cq = BagComp(Tup("E" -> Proj(TupleVar(e), "name"), "C" -> Proj(TupleVar(c), "name")), 
              List(Generator(e, InputR(employees.n, employees.tuples, employees.tp)), 
                   Generator(c, Proj(TupleVar(e), "children").asInstanceOf[BagCalc])))
       assert(tq.normalize == cq)
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
      val unq = Term(Reduce(Tup("E" -> Proj(TupleVar(e), "name"), "C" -> Proj(TupleVar(c), "name")), List(c,e), nopreds),
                      Term(Unnest(List(c, e), Proj(TupleVar(e), "children").asInstanceOf[BagCalc], nopreds), 
                        Select(InputR(employees.n, employees.tuples, employees.tp), e, nopreds)))
      assert(ncq == unq) 
      
    }

    /**
      * Query A unnesting example from F & M (7.1)
      */
    test("Unnester.unnest.F&M.QueryA"){

      val dtype = TupleType("dno" -> IntType)
      val etype = TupleType("dno" -> IntType)
      val e = VarDef("e", etype)
      val d = VarDef("d", dtype)
      val employees = Relation("Employees", List(
                        Map("dno" -> 1), Map("dno" -> 2), Map("dno" -> 3), Map("dno" -> 4)), BagType(etype))
      val departments = Relation("Departments", List(
                          Map("dno" -> 1), Map("dno" -> 2), Map("dno" -> 3), Map("dno" -> 4)), BagType(dtype))

      // For d in Departments Union 
      //  sng( D := d.dno, E := For e in Employees Union 
      //                          If (e.dno = d.dno)
      //                          Then sng(e)
      val q = ForeachUnion(d, departments, 
                Singleton(Tuple("D" -> Project(TupleVarRef(d), "dno"), "E" -> ForeachUnion(e, employees, 
                                                      IfThenElse(
                                                        Cond(OpEq, Project(TupleVarRef(e), "dno"), Project(TupleVarRef(d), "dno")), 
                                                                 Singleton(TupleVarRef(e)))))))

      // { ( D = d.dno, E = { e | e <- Employees, e.dno = d.dno } | d <- Departments }
      val cq = BagComp(Tup("D" -> Proj(TupleVar(d), "dno"), "E" -> BagComp(TupleVar(e), 
                                                  List(Generator(e, InputR(employees.n, employees.tuples, employees.tp)),
                                                       Conditional(OpEq, Proj(TupleVar(e), "dno"), Proj(TupleVar(d), "dno"))))),
                List(Generator(d, InputR(departments.n, departments.tuples, departments.tp))))
      assert(cq == cq.normalize)
      val tcq = Translator.translate(q)
      assert(tcq == cq)

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
      val v = VarDef("v", BagType(etype), VarCnt.currId+1)
      val nq = Unnester.unnest(cq.normalize)
      assert(nq.tp == BagType(TupleType("D" -> IntType, "E" -> BagType(TupleType("dno" -> IntType)))))
      val nopreds = List[PrimitiveCalc]()
      val unq = Term(Reduce(Tup("D" -> Proj(TupleVar(d), "dno"), "E" -> BagVar(v)), List(v, d), nopreds),                    
                      Term(Nest(TupleVar(e), List(e, d), List(d), nopreds, List(e)),
                        Term(OuterJoin(List(e,d), List(Conditional(OpEq, Proj(TupleVar(e), "dno"), Proj(TupleVar(d), "dno")))), 
                          Term(Select(InputR(employees.n, employees.tuples, employees.tp), e, nopreds), 
                            Select(InputR(departments.n, departments.tuples, employees.tp), d, nopreds)))))
      assert(nq == unq)
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
       */
 
}
