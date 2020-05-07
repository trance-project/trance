package framework.examples.simple

import framework.examples.Query
import framework.common._
import framework.nrc.MaterializeNRC

trait ExtractTest extends Query {
  def inputTypes(shred: Boolean = false): Map[Type, String] = Map[Type,String]()
  def headerTypes(shred: Boolean = false): List[String] = List[String]()

  def typeR = TupleType("a" -> BagType(TupleType("d" -> IntType)), "c" -> IntType)
  def typeS = TupleType("b" -> IntType)
  def typeT = TupleType("d" -> IntType)

  val relR = BagVarRef("R", BagType(typeR))
  val rr = TupleVarRef("x", typeR)

  val r2r = TupleVarRef("t", typeT)

  val relS = BagVarRef("S", BagType(typeS))
  val sr = TupleVarRef("s", typeS)

  val relT = BagVarRef("T", BagType(typeT))
  val tr = TupleVarRef("y", typeT)
}

object ExtractExample extends ExtractTest {
  val name = "ExtractExample"

  def inputs(tmap: Map[String,String]): String = ""

  val query1 =
    Union(
      ForeachUnion(rr, relR,
        Singleton(Tuple("myatt" ->
          ForeachUnion(r2r, BagProject(rr, "a"),
            IfThenElse(Cmp(OpEq, r2r("d"), Const(5, IntType)),
              Singleton(r2r))))))
      ,
      ForeachUnion(sr, relS,
        Singleton(Tuple("myatt" -> ForeachUnion(tr, relT,
          IfThenElse(Cmp(OpEq, tr("d"), sr("b")),
            Singleton(tr))))))
    )

  val program = Program(Assignment(name + "1", query1))

  val query2 =
    ForeachUnion(rr, relR,
      Singleton(Tuple(
        "x" -> PrimitiveProject(rr, "c"),
        "y" -> Union(
          ForeachUnion(sr, relS, Singleton(Tuple("myattr" -> sr("b")))),
          ForeachUnion(tr, relT, Singleton(Tuple("myattr" -> tr("d"))))
        )))
    )

  val program2 = Program(Assignment(name + "2", query2))

  val query3 =
    ForeachUnion(rr, relR,
      Singleton(Tuple(
        "x" -> PrimitiveProject(rr, "c"),
        "y" -> Union(
          ForeachUnion(r2r, BagProject(rr, "a"),
            IfThenElse(Cmp(OpEq, r2r("d"), Const(5, IntType)),
              Singleton(Tuple("myattr" -> r2r("d"))))),
          ForeachUnion(r2r, BagProject(rr, "a"),
            ForeachUnion(sr, relS,
              IfThenElse(Cmp(OpEq, r2r("d"), sr("b")),
                Singleton(Tuple("myattr" -> sr("b"))))))
        )))
    )

  val program3 = Program(Assignment(name + "3", query3))
}

object NestedTests {

  val nrc = new MaterializeNRC{}
  import nrc._
  
  // Relation 1

  val relR = BagVarRef("R", BagType(NestedRelations.type1a))
  val xref = TupleVarRef("x", NestedRelations.type1a)
  val wref = TupleVarRef("w", NestedRelations.type2a)

  // michael's first grouping example
  val q1 = ForeachUnion(xref, relR,
            Singleton(Tuple(
              "o5" -> xref("h"),
              "o6" -> ForeachUnion(wref, BagProject(xref, "j"),
                        Singleton(Tuple(
                          "o7" -> wref("m"),
                          "o8" -> Count(BagProject(wref, "k"))))))))
  
  // shallow version of q1
  val q2 = ForeachUnion(xref, relR,
            Singleton(Tuple(
              "o5" -> xref("h"), 
              "o6" -> Count(xref("j").asInstanceOf[BagExpr]))))
  
  // filter at top level
  val q3 = ForeachUnion(xref, relR,
            IfThenElse(Cmp(OpGt, xref("h"), Const(60, IntType)),
              Singleton(Tuple("w" -> xref("j")))))
  
  // filter at top level and take multiplicity
  val q4 = ForeachUnion(xref, relR,
            IfThenElse(Cmp(OpGt, xref("h"), Const(60, IntType)),
              Singleton(Tuple("w" -> Count(xref("j").asInstanceOf[BagExpr])))))
  
  // self join and project to a bag  
  val x2ref = TupleVarRef("x2", NestedRelations.type1a)
  val q5 = ForeachUnion(xref, relR,
            ForeachUnion(x2ref, relR,
              IfThenElse(Cmp(OpEq, xref("h"), x2ref("h")),
                Singleton(Tuple("x" -> xref("j"), "x2" -> x2ref("j"))))))

  // Relation 2
  
  val x3r = TupleVarRef("x", NestedRelations.type21a)
  val x4r = TupleVarRef("x1", NestedRelations.type21a)
  val x5r = TupleVarRef("x2", NestedRelations.type22a)
  val relR2 = BagVarRef("R", BagType(NestedRelations.type21a))

  // filter and multiplicity
  val q6 = ForeachUnion(x3r, relR2,
            IfThenElse(Cmp(OpGt, x3r("a"), Const(40, IntType)),
              Singleton(Tuple("o1" -> x3r("a"), "o2" -> Count(BagProject(x3r, "b"))))))

  // self join, and loop over a bag project 
  val q7 = ForeachUnion(x3r, relR2,
            ForeachUnion(x4r, relR2,
              ForeachUnion(x5r, BagProject(x3r, "b"),
                Singleton(Tuple("o1" -> x3r("a"), "o2" -> Count(BagProject(x3r, "b")))))))
    
   // michael's filter inner bag example
   val q8 = ForeachUnion(x3r, relR2,
              Singleton(Tuple("a" -> x3r("a"),
                "s1" -> ForeachUnion(x5r, BagProject(x3r, "b"),
                 IfThenElse(Cmp(OpGt, Const(2, IntType), x5r("c")), Singleton(x5r))))))
   // Relation 3
  
   // filter inner bag when let is shared
   val x6r = TupleVarRef("x", NestedRelations.type31a)
   val x7r = TupleVarRef("x1", NestedRelations.type32a)
   val relR3 = BagVarRef("R", BagType(NestedRelations.type31a))

   val q9 = ForeachUnion(x6r, relR3,
              Singleton(Tuple("a'" -> x6r("a"),
                "s1'" -> ForeachUnion(x7r, BagProject(x6r, "s1"),
                          IfThenElse(Cmp(OpGt, Const(5, IntType), x7r("c")), Singleton(x7r))),
                "s2'" -> ForeachUnion(x7r, BagProject(x6r, "s2"),
                          IfThenElse(Cmp(OpGt, x7r("c"), Const(6, IntType)),
                            Singleton(x7r))))))
    


    val q10name = "Test"
    val relR4 = BagVarRef("R", BagType(NestedRelations.type4a))
    val rx8 = TupleVarRef("x", NestedRelations.type4a)
    val rx9 = TupleVarRef("y", NestedRelations.type4b)
    val rx10 = TupleVarRef("z", NestedRelations.type4e)
    val q10 = ForeachUnion(rx8, relR4,
                Singleton(Tuple("o1" -> rx8("a"), "o2" ->
                  Count(ForeachUnion(rx9, BagProject(rx8, "b"),
                          IfThenElse(Cmp(OpEq, 
                                         Count(ForeachUnion(rx10, BagProject(rx8, "e"),
                                                IfThenElse(Cmp(OpEq, rx9("d"), rx10("g")), 
                                                  Singleton(Tuple("flag" -> Const("exists", StringType)))))),
                                          Const(0, IntType)),
                                      Singleton(Tuple("o3" -> rx8("a"))))))))) 

    val q11name = "Query5Test"
    val relP = BagVarRef("P", BagType(NestedRelations.typeP))
    val pr = TupleVarRef("p", NestedRelations.typeP)
    val relPS = BagVarRef("PS", BagType(NestedRelations.typePS))
    val psr = TupleVarRef("ps", NestedRelations.typePS)
    val relS = BagVarRef("S", BagType(NestedRelations.typeS))
    val sr = TupleVarRef("s", NestedRelations.typeS)
    val relL = BagVarRef("L", BagType(NestedRelations.typeL))
    val lr = TupleVarRef("l", NestedRelations.typeL)
    val relO = BagVarRef("O", BagType(NestedRelations.typeO))
    val or = TupleVarRef("o", NestedRelations.typeO)
    val relC = BagVarRef("C", BagType(NestedRelations.typeC))
    val cr = TupleVarRef("c", NestedRelations.typeC)

    val q11 = ForeachUnion(pr, relP,
                Singleton(Tuple("p_name" -> pr("p_name"), "suppliers" -> ForeachUnion(psr, relPS,
                  IfThenElse(Cmp(OpEq, psr("ps_partkey"), pr("p_partkey")),
                    ForeachUnion(sr, relS,
                      IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")),
                        Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"))))))),
                  "customers" -> ForeachUnion(lr, relL,
                    IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                      ForeachUnion(or, relO,
                        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                          ForeachUnion(cr, relC,
                            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                              Singleton(Tuple("c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"))))))))))))
}
