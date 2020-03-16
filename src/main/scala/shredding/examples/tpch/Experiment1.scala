package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.LinearizedNRC

/** Experiment 1.1, building nested structures **/

object Test0 extends TPCHBase {

  val name = "Test0"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(l, relL,
    Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))

}

object Test1 extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(o, relO,
    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(l, relL,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
          Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))

}

object Test2 extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(c, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
      ForeachUnion(o, relO,
        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
          Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
            ForeachUnion(l, relL,
              IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))))))
}

object Test2Flat extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(l, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)
  val query = 
  Sequence(List(Named(orders, oquery),
    ForeachUnion(c, relC,
      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))))
}

object Test3 extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(n, relN,
    Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
      ForeachUnion(c, relC,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
          Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
            ForeachUnion(o, relO,
              IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                  ForeachUnion(l, relL,
                    IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                      Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                )))))))))
}

object Test3Flat extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(l, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(c, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customer, customerRef) = varset("customers", "customer", cquery)
  val query = 
  Sequence(List(Named(orders, oquery), Named(customers, cquery), 
    ForeachUnion(n, relN, 
      Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" -> 
        ForeachUnion(customer, BagVarRef(customers),
          IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))))
}

object Test4 extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(r, relR,
    Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" ->
      ForeachUnion(n, relN,
        IfThenElse(Cmp(OpEq, nr("n_regionkey"), rr("r_regionkey")),
          Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
            ForeachUnion(c, relC,
              IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
                  ForeachUnion(o, relO,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                        ForeachUnion(l, relL,
                          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                      )))))))))))))
}

object Test4Flat extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R").contains(x._1)).values.toList.mkString("")}"
 
  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(l, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(c, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customer, customerRef) = varset("customers", "customer", cquery)

  val nquery = ForeachUnion(n, relN, 
    Singleton(Tuple("n_regionkey" -> nr("n_regionkey"), "n_name" -> nr("n_name"), "n_custs" -> 
      ForeachUnion(customer, BagVarRef(customers), 
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")), 
          Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))
  val (nations, nation, nationRef) = varset("nations", "nation", nquery)

  val query = 
  Sequence(List(Named(orders, oquery), Named(customers, cquery), Named(nations, nquery),
    ForeachUnion(r, relR,
      Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" -> 
        ForeachUnion(nation, BagVarRef(nations),
          IfThenElse(Cmp(OpEq, rr("r_regionkey"), nationRef("n_regionkey")),
            Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" -> nationRef("n_custs"))))))))))

}

/** Experiment 1.2, nested joins **/

object Test0Join extends TPCHBase {

  val name = "Test0"
  override def indexedDict: List[String] = List(s"${name}__D_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(l, relL,
    ForeachUnion(p, relP,
      IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))

}

object Test1Join extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(o, relO,
    Singleton(Tuple("orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(l, relL,
        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
          ForeachUnion(p, relP,
            IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))

}

object Test1JoinFlat extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(l, relL,
      ForeachUnion(p, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, part, partRef) = varset("parts", "part", pquery)

  val query = Sequence(List(Named(parts, pquery),
  ForeachUnion(o, relO,
    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
      ForeachUnion(part, BagVarRef(parts),
        IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
          Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))))

}

object Test2Join extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(c, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
      ForeachUnion(o, relO,
        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
          Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
            ForeachUnion(l, relL,
              IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                ForeachUnion(p, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))
}

object Test2JoinFlat extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  
  val pquery = 
    ForeachUnion(l, relL,
      ForeachUnion(p, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, part, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(part, BagVarRef(parts),
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)

  val query = 
  Sequence(List(Named(parts, pquery), Named(orders, oquery),
    ForeachUnion(c, relC,
      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))))
}

object Test3Join extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(n, relN,
    Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
      ForeachUnion(c, relC,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
          Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
            ForeachUnion(o, relO,
              IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                  ForeachUnion(l, relL,
                    IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                      ForeachUnion(p, relP,
                        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                          Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))
                  )))))))))))
}

object Test3JoinFlat extends TPCHBase {

  val name = "Test3"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(l, relL,
      ForeachUnion(p, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, part, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(part, BagVarRef(parts),
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(c, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customer, customerRef) = varset("customers", "customer", cquery)
  val query = 
  Sequence(List(Named(parts, pquery), Named(orders, oquery), Named(customers, cquery), 
    ForeachUnion(n, relN, 
      Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" -> 
        ForeachUnion(customer, BagVarRef(customers),
          IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))))
}

object Test4Join extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(r, relR,
    Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" ->
      ForeachUnion(n, relN,
        IfThenElse(Cmp(OpEq, nr("n_regionkey"), rr("r_regionkey")),
          Singleton(Tuple("n_name" -> nr("n_name"), "n_custs" ->
            ForeachUnion(c, relC,
              IfThenElse(Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
                  ForeachUnion(o, relO,
                    IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                      Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" ->
                        ForeachUnion(l, relL,
                          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                            Singleton(Tuple("p_name" -> lr("l_partkey"), "l_qty" -> lr("l_quantity")))))))
                      )))))))))))))
}

object Test4JoinFlat extends TPCHBase {

  val name = "Test4"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"n__Dr_nations_1", s"n__Dn_custs_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "N", "R", "P").contains(x._1)).values.toList.mkString("")}"
 
  val pquery = 
    ForeachUnion(l, relL,
      ForeachUnion(p, relP,
        IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("l_orderkey" -> lr("l_orderkey"),
            "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (parts, part, partRef) = varset("parts", "part", pquery)

  val oquery = 
    ForeachUnion(o, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(part, BagVarRef(parts),
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  val (orders, order, orderRef) = varset("orders", "order", oquery)

  val cquery = 
    ForeachUnion(c, relC,
      Singleton(Tuple("c_nationkey" -> cr("c_nationkey"), "c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(order, BagVarRef(orders),
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))
  val (customers, customer, customerRef) = varset("customers", "customer", cquery)

  val nquery = ForeachUnion(n, relN, 
    Singleton(Tuple("n_regionkey" -> nr("n_regionkey"), "n_name" -> nr("n_name"), "n_custs" -> 
      ForeachUnion(customer, BagVarRef(customers), 
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), customerRef("c_nationkey")), 
          Singleton(Tuple("c_name" -> customerRef("c_name"), "c_orders" -> customerRef("c_orders"))))))))
  val (nations, nation, nationRef) = varset("nations", "nation", nquery)

  val query = 
  Sequence(List(Named(parts, pquery), Named(orders, oquery), Named(customers, cquery), Named(nations, nquery),
    ForeachUnion(r, relR,
      Singleton(Tuple("r_name" -> rr("r_name"), "r_nations" -> 
        ForeachUnion(nation, BagVarRef(nations),
          IfThenElse(Cmp(OpEq, rr("r_regionkey"), nationRef("n_regionkey")),
            Singleton(Tuple("n_name" -> nationRef("n_name"), "n_custs" -> nationRef("n_custs"))))))))))

}

