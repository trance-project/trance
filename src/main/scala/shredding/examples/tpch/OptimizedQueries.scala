package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query

/**
  This file contains exploratory queries on tpch 
**/

object TPCHQueryCustOrders extends TPCHBase {
  val name = "CustOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  // input data
  val queryCO = ForeachUnion(c, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "corders" -> ForeachUnion(o, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate"))))))))

  val program = Program(Assignment(name, queryCO))
}

object TPCHQuery4A extends TPCHBase {
  val name = "Query4"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1, co, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val co2 = VarDef("order", BagProject(cor, "corders").tp.tp)
  val cor2 = TupleVarRef(co2)

  val parts = ForeachUnion(l, relL, 
                ForeachUnion(p, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1, co1, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])
  val query4 = ForeachUnion(co, BagVarRef(q1),
                Singleton(Tuple("c_name" -> cor("c_name"), "partqty" ->
                  GroupBy(
                       ForeachUnion(co2, BagProject(cor, "corders"),
                        ForeachUnion(co1, BagVarRef(p1),
                            IfThenElse(Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
                              Singleton(Tuple("orderdate" -> cor2("o_orderdate"),
                                "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty")))))),
                    List("orderdate", "pname"),
                    List("l_qty"),
                    DoubleType
                   ))))

  val program = Program(Assignment(p1.name, parts), Assignment(name, query4))
}

object TPCHQuery4PartOrders extends TPCHBase {
  val name = "PartOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2orders_1")

  // input data
  val query4 = ForeachUnion(l, relL,
                ForeachUnion(p, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"), "orders" -> 
                      ForeachUnion(o, relO,
                        IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("o_custkey" -> or("o_custkey"), "orderdate" -> or("o_orderdate"))))))))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4B extends TPCHBase {
  val name = "Query4"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2orders_1")

  val (q1, co, cor) = varset(TPCHQuery4PartOrders.name, "part",
    TPCHQuery4PartOrders.program(TPCHQuery4PartOrders.name).varRef.asInstanceOf[BagExpr])

  val co2 = VarDef("order", BagProject(cor, "orders").tp.tp)
  val cor2 = TupleVarRef(co2)

  val query4 =
      GroupBy(ForeachUnion(co, BagVarRef(q1), 
              ForeachUnion(co2, BagProject(cor, "orders"),
                ForeachUnion(c, relC, 
                  IfThenElse(Cmp(OpEq, cr("c_custkey"), cor2("o_custkey")),
                    Singleton(Tuple("c_name" -> cr("c_name"), "p_name" -> cor("p_name"), "l_qty" -> cor("l_qty"))))))),
        List("c_name", "p_name"),
        List("l_qty"),
        DoubleType)

  val program = Program(Assignment(name, query4))
}


// nested to flat that does an intermedite join
object TPCHQuery4C extends TPCHBase {
  val name = "Query4C"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1, co, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val co2 = VarDef("order", BagProject(cor, "corders").tp.tp)
  val cor2 = TupleVarRef(co2)

  val parts = ForeachUnion(l, relL, 
                ForeachUnion(p, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1, co1, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])
  val query4 = GroupBy(ForeachUnion(co, BagVarRef(q1),
                    ForeachUnion(co2, BagProject(cor, "corders"),
                      ForeachUnion(co1, BagVarRef(p1),
                        IfThenElse(Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
                          Singleton(Tuple("c_name" -> cor("c_name"), "orderdate" -> cor2("o_orderdate"), 
                          "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty"))))))),
      List("c_name", "orderdate", "pname"), 
      List("l_qty"),
      DoubleType)

  val program = Program(Assignment(p1.name, parts), Assignment(name, query4))
}

object TPCHQuery4D extends TPCHBase {
  val name = "Query4D"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1, co, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val co2 = VarDef("order", BagProject(cor, "corders").tp.tp)
  val cor2 = TupleVarRef(co2)

  val parts = ForeachUnion(l, relL, 
                ForeachUnion(p, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1, co1, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])

  val query4 = GroupBy(ForeachUnion(co, BagVarRef(q1),
              ForeachUnion(co2, BagProject(cor, "corders"),
                ForeachUnion(co1, BagVarRef(p1),
                  IfThenElse(Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
                    Singleton(Tuple("orderdate" -> cor2("o_orderdate"), 
                          "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty"))))))),
      List("orderdate", "pname"), 
      List("l_qty"),
      DoubleType)

  val program = Program(Assignment(p1.name, parts), Assignment(name, query4))
}

object TPCHQueryInputs extends TPCHBase {
  val name = "CustOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val parts = ForeachUnion(l, relL, 
                Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "parts" ->
                    ForeachUnion(p, relP,
                      IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))
  val (p1, co2, cor2) = varset("parts", "part", parts.asInstanceOf[BagExpr])

  val queryCO = ForeachUnion(c, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "corders" -> ForeachUnion(o, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate")))))))) 

  // input data
  val program = Program(Assignment(p1.name, parts), Assignment(name, queryCO))
}

object TPCHQuery4E extends TPCHBase {
  val name = "Query4E"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1, co, cor) = varset(TPCHQueryInputs.name, "customer",
    TPCHQueryInputs.program(TPCHQueryInputs.name).varRef.asInstanceOf[BagExpr])

  val co1 = VarDef("order", BagProject(cor, "corders").tp.tp)
  val cor1 = TupleVarRef(co1)

  val (p1, co2, cor2) = varset("parts", "part", TPCHQueryInputs.parts.asInstanceOf[BagExpr])
  val co3 = VarDef("q", BagProject(cor2, "parts").tp.tp)
  val cor3 = TupleVarRef(co3)

  //val query = Sequence(List(
 //   Named(p1, parts),
  val query4 = GroupBy(
      ForeachUnion(co, BagVarRef(q1),
        ForeachUnion(co1, BagProject(cor, "corders"),
          ForeachUnion(co2, BagVarRef(p1),
          IfThenElse(Cmp(OpEq, cor1("o_orderkey"), cor2("l_orderkey")),
            ForeachUnion(co3, BagProject(cor2, "parts"),
              Singleton(Tuple("c_name" -> cor("c_name"), "orderdate" -> cor1("o_orderdate"), 
              "pname" -> cor3("p_name"), "l_qty" -> cor3("l_qty")))))))),
      List("c_name", "orderdate", "pname"), 
      List("l_qty"),
      DoubleType
    )

  val program = Program(Assignment(name, query4))
}

object TPCHQuery7A extends TPCHBase {
  val name = "Query7"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x =>
      List("C", "O", "L", "P", "PS", "S", "N").contains(x._1)).values.toList.mkString("")}"

  val (q3, co, cor) = varset(TPCHQuery3Full.name, "co",
    TPCHQuery3Full.program(TPCHQuery3Full.name).varRef.asInstanceOf[BagExpr])

  val customers = BagProject(cor, "customers")
  val c2 = VarDef.fresh(customers.tp.tp)
  val c2r = TupleVarRef(c2)

  val suppliers = BagProject(cor, "suppliers")
  val s2 = VarDef.fresh(suppliers.tp.tp)
  val s2r = TupleVarRef(s2)

  val query7 = ForeachUnion(n, relN,
                Singleton(Tuple("n_name" -> nr("n_name"), "parts" ->
                  ForeachUnion(co, BagVarRef(q3),
                    Singleton(Tuple("p_name" -> cor("p_name"), "suppliers" ->
                      ForeachUnion(s2, suppliers,
                        IfThenElse(Cmp(OpEq, s2r("s_nationkey"), nr("n_nationkey")),
                          Singleton(Tuple("s_name" -> s2r("s_name"))))), "customers" -> 
                      ForeachUnion(c2, customers,
                        IfThenElse(Cmp(OpEq, c2r("c_nationkey"), nr("n_nationkey")),
                          Singleton(Tuple("c_name" -> c2r("c_name")))))))))))

  val program = Program(Assignment(name, query7))
}

/**
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,233259,query,app-20191228053552-0143
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,34609,unshredding,app-20191228053552-0143
ShredQuery4NewSparksfs100,/nfs_qc4/tpch/sfs100/,267874,total,app-20191228053552-0143
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,328660,query,app-20191228055403-0144
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,34901,unshredding,app-20191228055403-0144
ShredQuery4NewSparksfs100,/nfs_qc4/tpch/sfs100/,363565,total,app-20191228055403-0144
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,267758,query,app-20191228061328-0145
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,34509,unshredding,app-20191228061328-0145
ShredQuery4NewSparksfs100,/nfs_qc4/tpch/sfs100/,302271,total,app-20191228061328-0145
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,238968,query,app-20191228063300-0146
ShredQuery4NewSpark,sfs100,/nfs_qc4/tpch/sfs100/,34586,unshredding,app-20191228063300-0146
ShredQuery4NewSparksfs100,/nfs_qc4/tpch/sfs100/,273559,total,app-20191228063300-0146
Query4SparkManualsfs100,/nfs_qc4/tpch/sfs100/,283293,app-20191229044744-0147
Query4SparkManualsfs100,/nfs_qc4/tpch/sfs100/,245365,app-20191229050034-0148
Query4SparkManualsfs100,/nfs_qc4/tpch/sfs100/,261253,app-20191229051229-0149
Query4SparkManualsfs100,/nfs_qc4/tpch/sfs100/,284540,app-20191229052404-0150
**/
object TPCHQuery4New extends TPCHBase {
  val name = "Query4New"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query4 = ForeachUnion(co, BagVarRef(q1),
                Singleton(Tuple("c_name" -> cor("c_name"), "totals" ->
                  GroupBy(
                    ForeachUnion(co2, orders,
                      ForeachUnion(co3, parts,
                        Singleton(Tuple("orderdate" -> co2r("o_orderdate"),
                          "pname" -> co3r("p_name"), "qty" -> co3r("l_qty"))))),
                 List("orderdate", "pname"),
                 List("qty"),
                 DoubleType))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4New2 extends TPCHBase {
  val name = "Query4New2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query4 =
    ForeachUnion(co, BagVarRef(q1),
      ForeachUnion(co2, orders, 
        Singleton(Tuple("orders" -> co2r("o_orderdate"), "customers" ->
          ForeachUnion(c, relC,
            IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
              Singleton(Tuple("name" -> cr("c_name"), "address" -> cr("c_address")))))))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4New3 extends TPCHBase {
  val name = "Query4New3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query4 =
    GroupBy(
      ForeachUnion(co, BagVarRef(q1),
        ForeachUnion(co2, orders, 
          ForeachUnion(co3, parts, 
            ForeachUnion(p, relP, 
              IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                                "qty" -> co3r("l_qty"), "price" -> pr("p_retailprice")))))))),
      List("c_name", "p_name", "qty"),
      List("price"),
      DoubleType)

  val program = Program(Assignment(name, query4))
}

// transpose on the third level
object TPCHQuery4New4 extends TPCHBase {
  val name = "Query4New3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query4 =
    ForeachUnion(co, BagVarRef(q1),
      ForeachUnion(co2, orders, 
        ForeachUnion(co3, parts, 
          Singleton(Tuple("p_name" -> co3r("p_name"), "customers" -> 
            ForeachUnion(c, relC,
              IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
                Singleton(Tuple("customer" -> cr("c_name"), "address" -> cr("c_address"))))))))))

  val program = Program(Assignment(name, query4))
}


