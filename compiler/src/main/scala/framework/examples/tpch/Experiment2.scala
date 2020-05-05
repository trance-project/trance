package framework.examples.tpch

import framework.core._
import framework.examples.Query
import framework.nrc.MaterializeNRC

/** Flat to nested queries:
  * Suppliers - ( Customers JOIN Orders JOIN Lineitem )
  * Useful for skew
  */

object TestFN0 extends TPCHBase {
  val name = "TestFN0"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L").contains(x._1)).values.toList.mkString("")}"""
  override def indexedDict: List[String] = List(s"${name}__D_1")

  val custs = 
      ForeachUnion(or, relO,
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("c_orderkey" -> or("o_orderkey"),
                 "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate"))))))
  val (customers, customersRef) = varset("customers", "co", custs)

  val query5 = ForeachUnion(customersRef, customers, 
    ForeachUnion(lr, relL,
      IfThenElse(Cmp(OpEq, lr("l_orderkey"), customersRef("c_orderkey")),
        projectTuple(customersRef, Map("c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), 
          "l_quantity" -> lr("l_quantity")), List("c_orderkey")))))
                      
  val program = Program(Assignment(customers.name, custs), Assignment(name, query5))
}

object TestFN1 extends TPCHBase {
  val name = "TestFN1"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"""
    // List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_customers2_1")

  val custs = 
      ForeachUnion(or, relO,
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("c_orderkey" -> or("o_orderkey"),
                 "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate"))))))
  val (customers, customersRef) = varset("customers", "co", custs)

  val custsKeyed = ForeachUnion(customersRef, customers, 
    ForeachUnion(lr, relL,
      IfThenElse(Cmp(OpEq, lr("l_orderkey"), customersRef("c_orderkey")),
        projectTuple(customersRef, Map("c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), 
          "l_quantity" -> lr("l_quantity")), List("c_orderkey")))))
  val (csuppkeys, csuppRef) = varset("csupps", "cs", custsKeyed)
                       
  val query5 = ForeachUnion(sr, relS,
                Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> 
                  ForeachUnion(csuppRef, csuppkeys,
                    IfThenElse(Cmp(OpEq, csuppRef("c_suppkey"), sr("s_suppkey")),
                      projectBaseTuple(csuppRef, List("c_suppkey"))))))) 

  val program = Program(Assignment(customers.name, custs), 
    Assignment(csuppkeys.name, custsKeyed), Assignment(name, query5))
}

object TestFN2 extends TPCHBase {
  val name = "TestFN2"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S", "N").contains(x._1)).values.toList.mkString("")}"""
    // List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_customers2_1", s"${name}__D_1_n_nations_1_customers2_1")

  val custs = 
      ForeachUnion(or, relO,
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("c_orderkey" -> or("o_orderkey"),
                 "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate"))))))
  val (customers, customersRef) = varset("customers", "co", custs)

  val custsKeyed = ForeachUnion(customersRef, customers, 
    ForeachUnion(lr, relL,
      IfThenElse(Cmp(OpEq, lr("l_orderkey"), customersRef("c_orderkey")),
        projectTuple(customersRef, Map("c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), 
          "l_quantity" -> lr("l_quantity")), List("c_orderkey")))))
  val (csuppkeys, csuppRef) = varset("csupps", "cs", custsKeyed)

  val query5 = ForeachUnion(nr, relN, 
    Singleton(Tuple("n_name" -> nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> 
      ForeachUnion(sr, relS,
        Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> 
          ForeachUnion(csuppRef, csuppkeys,
            IfThenElse(Cmp(OpEq, csuppRef("c_suppkey"), sr("s_suppkey")),
              projectBaseTuple(csuppRef, List("c_suppkey"))))))))))

  val program = Program(Assignment(customers.name, custs), 
    Assignment(csuppkeys.name, custsKeyed), Assignment(name, query5))
}

object TestFN2Full extends TPCHBase {
  val name = "TestFN2"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S", "N").contains(x._1)).values.toList.mkString("")}"""
    // List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_customers2_1", s"${name}__D_1_n_nations_1_customers2_1")

  val custs = 
      ForeachUnion(or, relO,
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("c_orderkey" -> or("o_orderkey"),
                 "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate"))))))
  val (customers, customersRef) = varset("customers", "co", custs)

  val custsKeyed = ForeachUnion(customersRef, customers, 
    ForeachUnion(lr, relL,
      IfThenElse(Cmp(OpEq, lr("l_orderkey"), customersRef("c_orderkey")),
        projectTuple(customersRef, Map("c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), 
          "l_quantity" -> lr("l_quantity")), List("c_orderkey")))))
  val (csuppkeys, csuppRef) = varset("csupps", "cs", custsKeyed)
                       
  val suppsKeyed = ForeachUnion(sr, relS,
                Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> 
                  ForeachUnion(csuppRef, csuppkeys,
                    IfThenElse(Cmp(OpEq, csuppRef("c_suppkey"), sr("s_suppkey")),
                      projectBaseTuple(csuppRef, List("c_suppkey")))))))
  val (suppliers, suppliersRef) = varset("suppliers", "supplier", suppsKeyed) 

  val query5 = ForeachUnion(nr, relN, 
    Singleton(Tuple("n_name" -> nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> 
      ForeachUnion(suppliersRef, suppliers, 
        IfThenElse(Cmp(OpEq, suppliersRef("s_nationkey"), nr("n_nationkey")),
          projectBaseTuple(suppliersRef, List("s_nationkey")))))))

  val program = Program(Assignment(customers.name, custs), 
    Assignment(csuppkeys.name, custsKeyed), Assignment(suppliers.name, suppsKeyed), Assignment(name, query5))
}
