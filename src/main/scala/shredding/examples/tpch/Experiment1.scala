package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.MaterializeNRC

/** Experiment 1.1, building nested structures **/

object Test0 extends TPCHBase {

  val name = "Test0"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(lr, relL, 
    Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))

  val program = Program(Assignment(name, query))

}

object Test0Full extends TPCHBase {

  val name = "Test0Full"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L").contains(x._1)).values.toList.mkString("")}"
 
  val query = ForeachUnion(lr, relL, projectBaseTuple(lr))

  val program = Program(Assignment(name, query))

}

object Test1 extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(or, relO,
    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(lr, relL,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
          Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))

  val program = Program(Assignment(name, query))

}

object Test1Full extends TPCHBase {

  val name = "Test1Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(or, relO,
    projectTuple(or, "o_parts" ->
      ForeachUnion(lr, relL,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
          projectBaseTuple(lr)))))

  val program = Program(Assignment(name, query))

}

object Test2 extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_c_orders_1", 
    s"${name}__D_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(cr, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
      ForeachUnion(or, relO,
        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
          Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
            ForeachUnion(lr, relL,
              IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))))))
  val program = Program(Assignment(name, query))
}

object Test2Full extends TPCHBase {

  val name = "Test2Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_c_orders_1", 
    s"${name}__D_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(cr, relC,
    projectTuple(cr, "c_orders" -> 
      ForeachUnion(or, relO,
        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
          projectTuple(or, "o_parts" ->
            ForeachUnion(lr, relL,
              IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                projectBaseTuple(lr))))))))
  val program = Program(Assignment(name, query))
}

object Test2Flat extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_c_orders_1", 
    s"${name}__D_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)
  val query = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))

  val program = Program(Assignment(orders.name, oquery), Assignment(name, query))
}

object Test2FullFlat extends TPCHBase {

  val name = "Test2Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_c_orders_1", 
    s"${name}__D_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      projectTuple(or, "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            projectBaseTuple(lr)))))

  val (orders, orderRef) = varset("orders", "order", oquery)
  val query = 
    ForeachUnion(cr, relC,
      projectTuple(cr, "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            projectTuple(orderRef, "o_parts" -> orderRef("o_parts"), List("o_parts"))))))

  val program = Program(Assignment(orders.name, oquery), Assignment(name, query))
}

object Test3 extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_n_custs_1", 
    s"${name}__D_1_n_custs_1_c_orders_1", s"${name}__D_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(nr, relN,
    Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
      ForeachUnion(cr, relC,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
          Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
            ForeachUnion(or, relO,
              IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                  ForeachUnion(lr, relL,
                    IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                      Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                )))))))))

  val program = Program(Assignment(name, query))
}

object Test3Full extends TPCHBase {

  val name = "Test3Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_n_custs_1", 
    s"${name}__D_1_n_custs_1_c_orders_1", s"${name}__D_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(nr, relN,
    projectTuple(nr, "n_custs" ->
      ForeachUnion(cr, relC,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
          projectTuple(cr, "c_orders" -> 
            ForeachUnion(or, relO,
              IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                projectTuple(or, "o_parts" ->
                  ForeachUnion(lr, relL,
                    IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                      projectBaseTuple(lr)))))))))))

  val program = Program(Assignment(name, query))
}

object Test3Flat extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_n_custs_1", 
    s"${name}__D_1_n_custs_1_c_orders_1", s"${name}__D_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)
  val query = 
    ForeachUnion(nr, relN, 
      Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" -> 
        ForeachUnion(customerRef, customers,
          IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))
  val program = Program(Assignment(orders.name, oquery), Assignment(customers.name, cquery), Assignment(name, query))

}

object Test3FullFlat extends TPCHBase {

  val name = "Test3Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_n_custs_1", 
    s"${name}__D_1_n_custs_1_c_orders_1", s"${name}__D_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      projectTuple(or, "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            projectBaseTuple(lr)))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      projectTuple(cr, "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            projectTuple(orderRef, "o_parts" -> orderRef("o_parts"), List("o_parts"))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)
  val query = 
    ForeachUnion(nr, relN, 
      projectTuple(nr, "n_custs" -> 
        ForeachUnion(customerRef, customers,
          IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")),
            projectTuple(customerRef, "c_orders" -> customerRef("c_orders"), List("c_orders"))))))
  val program = Program(Assignment(orders.name, oquery), Assignment(customers.name, cquery), Assignment(name, query))

}

object Test4 extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_r_nations_1", 
    s"${name}__D_1_r_nations_1_n_custs_1", s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1", 
    s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(rr, relR,
    Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" ->
      ForeachUnion(nr, relN,
        IfThenElse(Cmp(OpEq, nr("n_regionkey"), rr("r_regionkey")),
          Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
            ForeachUnion(cr, relC,
              IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
                  ForeachUnion(or, relO,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                        ForeachUnion(lr, relL,
                          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                      )))))))))))))
  val program = Program(Assignment(name, query))
}

object Test4Full extends TPCHBase {

  val name = "Test4Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_r_nations_1", 
    s"${name}__D_1_r_nations_1_n_custs_1", s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1", 
    s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(rr, relR,
    projectTuple(rr, "r_nations" ->
      ForeachUnion(nr, relN,
        IfThenElse(Cmp(OpEq, nr("n_regionkey"), rr("r_regionkey")),
          projectTuple(nr, "n_custs" ->
            ForeachUnion(cr, relC,
              IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                projectTuple(cr, "c_orders" -> 
                  ForeachUnion(or, relO,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      projectTuple(or, "o_parts" ->
                        ForeachUnion(lr, relL,
                          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                            projectBaseTuple(lr))))))))))))))
  val program = Program(Assignment(name, query))
}

object Test4Flat extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_r_nations_1", 
    s"${name}__D_1_r_nations_1_n_custs_1", s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1", 
    s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)

  val nquery = ForeachUnion(nr, relN, 
    Singleton(Tuple("n_regionkey" -> nr("n_regionkey"), "n_name" -> nr("n_name"), "n_custs" -> 
      ForeachUnion(customerRef, customers, 
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")), 
          Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))
  val (nations, nationRef) = varset("nations", "nation", nquery)

  val query = 
    ForeachUnion(rr, relR,
      Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" -> 
        ForeachUnion(nationRef, nations,
          IfThenElse(Cmp(OpEq, rr("r_regionkey"), nationRef("n_regionkey")),
            Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" -> nationRef("n_custs"))))))))

  val program = Program(Assignment(orders.name, oquery), 
    Assignment(customers.name, cquery), Assignment(nations.name, nquery), Assignment(name, query))

}

object Test4FullFlat extends TPCHBase {

  val name = "Test4Full"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_r_nations_1", 
    s"${name}__D_1_r_nations_1_n_custs_1", s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1", 
    s"${name}__D_1_r_nations_1_n_custs_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(or, relO,
      projectTuple(or, "o_parts" -> 
        ForeachUnion(lr, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            projectBaseTuple(lr)))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      projectTuple(cr, "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            projectTuple(orderRef, "o_parts" -> orderRef("o_parts"), List("o_parts"))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)

  val nquery = ForeachUnion(nr, relN, 
    projectTuple(nr, "n_custs" -> 
      ForeachUnion(customerRef, customers, 
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")), 
          projectTuple(customerRef, "c_orders" -> customerRef("c_orders"), List("c_orders"))))))
  val (nations, nationRef) = varset("nations", "nation", nquery)

  val query = 
    ForeachUnion(rr, relR,
      projectTuple(rr, "r_nations" -> 
        ForeachUnion(nationRef, nations,
          IfThenElse(Cmp(OpEq, rr("r_regionkey"), nationRef("n_regionkey")),
            projectTuple(nationRef, "n_custs" -> nationRef("n_custs"), List("n_custs"))))))

  val program = Program(Assignment(orders.name, oquery), 
    Assignment(customers.name, cquery), Assignment(nations.name, nquery), Assignment(name, query))

}

/** Experiment 1.2, nested to nested with a join at bottom level **/

object Test0NN extends TPCHBase {

  val name = "Test0NN"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val partsInput = Test0.program(Test0.name).varRef.asInstanceOf[BagExpr]
  val (parts, partRef) = varset(Test0.name, "l", partsInput)
  val query = 
    ReduceByKey(ForeachUnion(partRef, parts,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_qty")))))),
    List("p_name"), List("l_quantity"))

  val program = Program(Assignment(name, query))

}

object Test0Push extends TPCHBase {

  val name = "Test0Push"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val partsInput = Test0Full.program(Test0Full.name).varRef.asInstanceOf[BagExpr]
  val (parts, partRef) = varset(Test0Full.name, "l", partsInput)
  val pushAgg = ReduceByKey(parts, List("l_partkey"), List("l_quantity"))
  val (partsAgg, partsAggRef) = varset("localAgg", "l2", pushAgg)
  val query = 
    ReduceByKey(ForeachUnion(partsAggRef, partsAgg,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))),
    List("p_name"), List("l_quantity"))

  val program = Program(Assignment(partsAgg.name, pushAgg), Assignment(name, query))

}

object Test0FullNN extends TPCHBase {

  val name = "Test0FullNN"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val partsInput = Test0Full.program(Test0Full.name).varRef.asInstanceOf[BagExpr]
  val (parts, partRef) = varset(Test0Full.name, "l", partsInput)
  val query = 
    ReduceByKey(ForeachUnion(partRef, parts,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))), 
    List("p_name"), List("l_quantity"))

  val program = Program(Assignment(name, query))

}

object Test1NN extends TPCHBase {

  val name = "Test1NN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (orders, orderRef) = varset(Test1.name, "o", Test1.program(Test1.name).varRef.asInstanceOf[BagExpr])
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
    ForeachUnion(orderRef, orders,
      Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), "o_parts" ->
        ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
          ForeachUnion(pr, relP,
            IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_qty")))))),
            List("p_name"), List("l_quantity")))))

  val program = Program(Assignment(name, query))

}

object Test1FullNN extends TPCHBase {

  val name = "Test1FullNN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (orders, orderRef) = varset(Test1Full.name, "o", Test1Full.program(Test1Full.name).varRef.asInstanceOf[BagExpr])
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
    ForeachUnion(orderRef, orders,
      projectTuple(orderRef, "o_parts" ->
        ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
          ForeachUnion(pr, relP,
            IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))),
            List("p_name"), List("l_quantity"))))

  val program = Program(Assignment(name, query))

}

object Test2NN extends TPCHBase {

  val name = "Test2NN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (customers, customerRef) = varset(Test2.name, "c", Test2.program(Test2.name).varRef.asInstanceOf[BagExpr])
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(customerRef, customers,
    Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> 
      ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
        Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), "o_parts" ->
          ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_qty")))))),
          List("p_name"), List("l_quantity"))))))))

  val program = Program(Assignment(name, query))
}

object Test2FullNN extends TPCHBase {

  val name = "Test2FullNN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (customers, customerRef) = varset(Test2Full.name, "c", Test2Full.program(Test2Full.name).varRef.asInstanceOf[BagExpr])
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(customerRef, customers,
    projectTuple(customerRef, "c_orders" -> 
      ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
        projectTuple(orderRef, "o_parts" ->
          ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))),
          List("p_name"), List("l_quantity"))))))
  val program = Program(Assignment(name, query))
}

object Test3NN extends TPCHBase {

  val name = "Test3NN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (nations, nationRef) = varset(Test3.name, "n", Test3.program(Test3.name).varRef.asInstanceOf[BagExpr])
  val (customers, customerRef) = varset("customers", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(nationRef, nations,
    Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" ->
      ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
        Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> 
          ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), "o_parts" ->
              ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_qty")))))),
                List("p_name"), List("l_quantity")))))))))))
  val program = Program(Assignment(name, query))
}

object Test3FullNN extends TPCHBase {

  val name = "Test3FullNN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (nations, nationRef) = varset(Test3Full.name, "n", Test3Full.program(Test3Full.name).varRef.asInstanceOf[BagExpr])
  val (customers, customerRef) = varset("customers", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(nationRef, nations,
    projectTuple(nationRef, "n_custs" ->
      ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
        projectTuple(customerRef, "c_orders" -> 
          ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
            projectTuple(orderRef, "o_parts" ->
              ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))),
              List("p_name"), List("l_quantity"))))))))
  val program = Program(Assignment(name, query))
}

object Test4NN extends TPCHBase {

  val name = "Test4NN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"r__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (regions, regionRef) = varset(Test4.name, "r", Test4.program(Test4.name).varRef.asInstanceOf[BagExpr])
  val (nations, nationRef) = varset("nations", "n", BagProject(regionRef, "r_nations"))
  val (customers, customerRef) = varset("customers", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(regionRef, regions,
    Singleton(Tuple("r_name" -> regionRef("r_name"), "r_nations" ->
      ForeachUnion(nationRef, BagProject(regionRef, "r_nations"),
        Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" ->
          ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> 
              ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
                Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), "o_parts" ->
                  ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
                    ForeachUnion(pr, relP,
                      IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_qty")))))),
                  List("p_name"), List("l_quantity"))))))))))))))
  val program = Program(Assignment(name, query))
}

object Test4FullNN extends TPCHBase {

  val name = "Test4FullNN"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"r__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val (regions, regionRef) = varset(Test4Full.name, "r", Test4Full.program(Test4Full.name).varRef.asInstanceOf[BagExpr])
  val (nations, nationRef) = varset("nations", "n", BagProject(regionRef, "r_nations"))
  val (customers, customerRef) = varset("customers", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))
  val query = 
  ForeachUnion(regionRef, regions,
    projectTuple(regionRef, "r_nations" ->
      ForeachUnion(nationRef, BagProject(regionRef, "r_nations"),
        projectTuple(nationRef, "n_custs" ->
          ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
            projectTuple(customerRef, "c_orders" -> 
              ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
                projectTuple(orderRef, "o_parts" ->
                  ReduceByKey(ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
                    ForeachUnion(pr, relP,
                      IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_quantity" -> partRef("l_quantity")))))),
                  List("p_name"), List("l_quantity"))))))))))
  val program = Program(Assignment(name, query))
}

/** Experiment 1.3, nested joins **/

object Test0Join extends TPCHBase {

  val name = "Test0"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(lr, relL,
    ForeachUnion(pr, relP,
      IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))

  val program = Program(Assignment(name, query))
}

object Test1Join extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(or, relO,
    Singleton(Tuple("orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(lr, relL,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
          ForeachUnion(pr, relP,
            IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))

  val program = Program(Assignment(name, query))
}

object Test1JoinFlat extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(lr, relL,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, partRef) = varset("parts", "part", pquery)

  val query = 
  ForeachUnion(or, relO,
    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(partRef, parts,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
          Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))

  val program = Program(Assignment(parts.name, pquery), Assignment(name, query))
}

object Test2Join extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(cr, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
      ForeachUnion(or, relO,
        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
          Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
            ForeachUnion(lr, relL,
              IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))
  val program = Program(Assignment(name, query))
}

object Test2JoinFlat extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val pquery = 
    ForeachUnion(lr, relL,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(partRef, parts,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val query = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val program = Program(Assignment(parts.name, pquery), 
    Assignment(orders.name, oquery), Assignment(name, query))
}

object Test3Join extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(nr, relN,
    Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
      ForeachUnion(cr, relC,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
          Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
            ForeachUnion(or, relO,
              IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                  ForeachUnion(lr, relL,
                    IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                      ForeachUnion(pr, relP,
                        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                          Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))
                  )))))))))))
  val program = Program(Assignment(name, query))
}

object Test3JoinFlat extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(lr, relL,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(partRef, parts,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)
  val query = 
    ForeachUnion(nr, relN, 
      Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" -> 
        ForeachUnion(customerRef, customers,
          IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))

  val program = Program(Assignment(parts.name, pquery), 
    Assignment(orders.name, oquery), Assignment(customers.name, cquery), 
      Assignment(name, query))
    
}

object Test4Join extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(rr, relR,
    Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" ->
      ForeachUnion(nr, relN,
        IfThenElse(Cmp(OpEq, nr("n_regionkey"), rr("r_regionkey")),
          Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
            ForeachUnion(cr, relC,
              IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
                  ForeachUnion(or, relO,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                        ForeachUnion(lr, relL,
                          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                            Singleton(Tuple("p_name" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                      )))))))))))))

  val program = Program(Assignment(name, query))
}

object Test4JoinFlat extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(lr, relL,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(partRef, parts,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customerRef) = varset("customers", "customer", cquery)

  val nquery = ForeachUnion(nr, relN, 
    Singleton(Tuple("n_regionkey" -> nr("n_regionkey"), "n_name" -> nr("n_name"), "n_custs" -> 
      ForeachUnion(customerRef, customers, 
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")), 
          Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))
  val (nations, nationRef) = varset("nations", "nation", nquery)

  val query = 
    ForeachUnion(rr, relR,
      Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" -> 
        ForeachUnion(nationRef, nations,
          IfThenElse(Cmp(OpEq, rr("r_regionkey"), nationRef("n_regionkey")),
            Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" -> nationRef("n_custs"))))))))

  val program = Program(Assignment(parts.name, pquery), Assignment(orders.name, oquery), 
    Assignment(customers.name, cquery), Assignment(nations.name, nquery),
    Assignment(name, query))
}

