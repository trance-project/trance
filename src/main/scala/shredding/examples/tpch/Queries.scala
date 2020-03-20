package shredding.examples.tpch

import shredding.core._
import shredding.nrc.MaterializeNRC

object TPCHQueries {

  val nrc = new MaterializeNRC{}
  import nrc._
  
  /**
    * var Q1 =
    * for (c <- Customer) yield (c.name,
    *   for (o <- Orders
    * if o.custkey == c.custkey)
    * yield (o.orderdate,
    * for (l <- Lineitem; p <- Part
    * if l.orderkey == o.orderkey && l.partkey == p.partkey)
    * yield (p.name, l.qty) ))
    */
  
  val relC = BagVarRef("C", TPCHSchema.customertype)
  val cr = TupleVarRef("c", TPCHSchema.customertype.tp)

  val relO = BagVarRef("O", TPCHSchema.orderstype)
  val or = TupleVarRef("o", TPCHSchema.orderstype.tp)

  val relL = BagVarRef("L", TPCHSchema.lineittype)
  val lr = TupleVarRef("l", TPCHSchema.lineittype.tp)

  val relP = BagVarRef("P", TPCHSchema.parttype)
  val pr = TupleVarRef("p", TPCHSchema.parttype.tp)


  val q0name = "Query0"
  val query0 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"))))))))
  /**, "o_parts" -> ForeachUnion(lr, relL,
                  IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                    ForeachUnion(pr, relP, IfThenElse(
                      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))
**/

  val q1name = "Query1"
  val q1data = s"""
    |val C = TPCHLoader.loadCustomer[Customer].toList
    |val O = TPCHLoader.loadOrders[Orders].toList
    |val L = TPCHLoader.loadLineitem[Lineitem].toList
    |val P = TPCHLoader.loadPart[Part].toList""".stripMargin

  val q1spark = s"""
    |val tpch = TPCHLoader(spark)
    |val C = tpch.loadCustomers
    |C.cache
    |C.count
    |val O = tpch.loadOrders
    |O.cache
    |O.count
    |val L = tpch.loadLineitem
    |L.cache
    |L.count
    |val P = tpch.loadPart
    |P.cache
    |P.count""".stripMargin
  
  val sq1spark = s"""
    |val tpch = TPCHLoader(spark)
    |val C__F = 1
    |val C__D_1 = tpch.loadCustomers()
    |C__D_1.cache
    |C__D_1.count
    |val O__F = 2
    |val O__D_1 = tpch.loadOrders()
    |O__D_1.cache
    |O__D_1.count
    |val L__F = 3
    |val L__D_1 = tpch.loadLineitem()
    |L__D_1.cache
    |L__D_1.count
    |val P__F = 4
    |val P__D_1 = tpch.loadPart()
    |P__D_1.cache
    |P__D_1.count""".stripMargin


  val sq1data = s"""
    |val C__F = 1
    |val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
    |val O__F = 2
    |val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
    |val L__F = 3
    |val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
    |val P__F = 4
    |val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())""".stripMargin
 
  val q1aname = "Query1A" 
  val ljpr = BagVarRef("ljp", BagType(TupleType("l_orderkey" -> IntType, "p_name" -> StringType, "l_qty" -> DoubleType)))
  val lpr = TupleVarRef("lp", TupleType("l_orderkey" -> IntType, "p_name" -> StringType, "l_qty" -> DoubleType))
  val query1a = Let(ljpr, ForeachUnion(lr, relL,
                          ForeachUnion(pr, relP, IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                            Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))),
                    ForeachUnion(cr, relC,
                      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                        IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                          Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                            ForeachUnion(lpr, ljpr,
                              IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                                Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty")))))))))))))

  /**val query1 = ForeachUnion(c, relC, 
            Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
              IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL, 
                  IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                    ForeachUnion(p, relP, IfThenElse(
                      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))**/
 
 // query 1 with extended schema for filtering in query 4 
 val query1b = ForeachUnion(cr, relC,
            Singleton(Tuple("c_id" -> cr("c_custkey"), "c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
              IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                Singleton(Tuple("o_id" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
                  IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                    ForeachUnion(pr, relP, IfThenElse(
                      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

  val q1btype = TupleType("c_id" -> IntType, "c_name" -> StringType, "c_orders" ->
                          BagType(TupleType("o_id" -> IntType, "o_orderdate" -> StringType, "o_parts" ->
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType)))))

  val query1 = ForeachUnion(cr, relC,
            Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
              IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
                  ForeachUnion(pr, relP, IfThenElse(
                    And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")), Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))),
                      Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))))))))

  val query1_v2 = ForeachUnion(cr, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(lr, relL,
          IfThenElse(
            Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
            ForeachUnion(pr, relP, IfThenElse(
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

  val q1type = TupleType("c_name" -> StringType, "c_orders" ->
                          BagType(TupleType("o_orderdate" -> StringType, "o_parts" ->
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType)))))

  
  val inputq4a = ForeachUnion(cr, relC,
                  Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> cr("c_custkey"))))
  val input4a = Assignment("Q1Flat1", inputq4a)

  val inputq4b = ForeachUnion(cr, relC,
                  Singleton(Tuple("_1" -> cr("c_custkey"), "_2" -> ForeachUnion(or, relO,
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      Singleton(Tuple("a" -> cr("c_custkey"), "b" -> or("o_orderkey")))))))))
  val input4b = Assignment("Q1Flat2", inputq4b)

  val inputq4c = ForeachUnion(cr, relC,
                  ForeachUnion(or, relO,
                    Singleton(Tuple("_1" -> Singleton(Tuple("a" -> cr("c_custkey"), "b" -> or("o_orderkey"))),
                      "_2" -> ForeachUnion(lr, relL,
                        IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                          ForeachUnion(pr, relP, IfThenElse(
                            Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))))))
  val input4c = Assignment("Q1Flat3", inputq4c)
  
  val input4 = Program(input4a, input4b, input4c)
  
  val q4name = "Query4"

  val Q1r = BagVarRef(q4name, query1.tp)
  val q1r = TupleVarRef("q1", q1type)
  val q2r = TupleVarRef("q12", q1type)

  val cq1r = TupleVarRef("corders",
    TupleType("o_orderdate" -> StringType, "o_parts" -> BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType))))
  val cq2r = TupleVarRef("corders2",
    TupleType("o_orderdate" -> StringType, "o_parts" -> BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType))))

  val pq1r = TupleVarRef("oparts", TupleType("p_name" -> StringType, "l_qty" -> DoubleType))
  val pq2r = TupleVarRef("oparts2", TupleType("p_name" -> StringType, "l_qty" -> DoubleType))

  val qt = VarDef("qt", IntType)

  /** 
    * need to add primitive support for month extraction
    *
    * var Q4 = (
    *   for ((c_name, c_orders) <- Q1; 
    *        (o_orderdate, o_parts) <- c_orders; 
    *        (p_name, l_qty) <- o_parts)
    *   yield ((c_name,p_name,getMonth(o_orderdate)),l_qty) ) 
    *    .reduceByKey(_ + _)
    */

  //val sq4cclass = (r1: String, r2: String) => s"""
  val sq4data = s"""
    |case class Input_Q1_Dict2(o_parts: (List[RecM_flat3], Unit))
    |case class Input_Q1_Dict1(c_orders: (List[RecM_flat2], Input_Q1_Dict2))
    |val Query4__F = ShredQuery4._1.head.lbl
    |val Query4__D = (ShredQuery4._2, Input_Q1_Dict1((ShredQuery4._4, Input_Q1_Dict2((ShredQuery4._6, Unit)))))""".stripMargin

  val sq4spark = s"""
      |val Query4__F = M_ctx1
      |val Query4__D_1 = M_flat1.flatMap{ r => r._2 }
      |Query4__D_1.cache
      |Query4__D_1.count
      |val Query4__D_2c_orders_1 = M_flat2
      |Query4__D_2c_orders_1.cache
      |Query4__D_2c_orders_1.count
      |val Query4__D_2c_orders_2o_parts = M_flat3
      |Query4__D_2c_orders_2o_parts.cache
      |Query4__D_2c_orders_2o_parts.count
    """
  /**val query4 = ForeachUnion(q1, BagVarRef(Q1), 
                  ForeachUnion(cq1, BagProject(q1r, "c_orders"), 
                    ForeachUnion(pq1, BagProject(cq1r, "o_parts"), 
                      Singleton(Tuple("c_name" -> q1r("c_name"), "p_name" -> pq1r("p_name"), "month" -> cq1r("o_orderdate"), 
                        "t_qty" -> Total(
			     ForeachUnion(q2, BagVarRef(Q1),
			       ForeachUnion(cq2, BagProject(q1r, "c_orders"),
			         ForeachUnion(pq2, BagProject(cq1r, "o_parts"), 
                                    IfThenElse(And(And(Cmp(OpEq, TupleVarRef(pq2)("p_name"), pq1r("p_name")), 
				     	Cmp(OpEq, TupleVarRef(cq2)("o_orderdate"), cq1r("o_orderdate"))),
					Cmp(OpEq, TupleVarRef(q2)("c_name"), q1r("c_name")))
                                  //IfThenElse(Cmp(OpEq, TupleVarRef(pq2)("p_name"), pq1r("p_name")),
                                    WeightedSingleton(Tuple("l_qty" -> pq1r("l_qty")), 
                                      TupleVarRef(pq2)("l_qty").asInstanceOf[PrimitiveExpr])))))))))))   **/

  val query4a =
    ForeachUnion(q1r, Q1r,
      ForeachUnion(cq1r, BagProject(q1r, "c_orders"),
        ForeachUnion(pq1r, BagProject(cq1r, "o_parts"),
          Singleton(Tuple("c_name" -> q1r("c_name"), "p_name" -> pq1r("p_name"), "month" -> cq1r("o_orderdate"),
            "t_qty" ->
              Sum(ForeachUnion(pq2r, BagProject(cq1r, "o_parts"),
                IfThenElse(Cmp(OpEq, pq2r("p_name"), pq1r("p_name")),
                  Singleton(Tuple("l_qty" -> pq2r("l_qty").asInstanceOf[NumericExpr])))),
                List("l_qty")
              )("l_qty")
          )))))

  val vd = VarDef("x", 
            TupleType("c_name" -> StringType, "p_name" -> StringType, 
                      "month" -> StringType, "t_qty" -> DoubleType))

  val Q1br = BagVarRef(q4name, query1b.tp)
  val q1br = TupleVarRef("q1", q1btype)

  val cq1br = TupleVarRef("c1", TupleType("o_id" -> IntType, "o_orderdate" -> StringType, "o_parts" -> BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType))))

  val query4c =
    SumByKey(
      ForeachUnion(q1br, Q1br,
        IfThenElse(
          Cmp(OpGt, Const(1000, IntType), q1br("c_id")),
          ForeachUnion(cq1br, BagProject(q1br, "c_orders"),
            IfThenElse(
              Cmp(OpGt, Const(1000, IntType), cq1br("o_id")),
              ForeachUnion(pq1r, BagProject(cq1br, "o_parts"),
                Singleton(Tuple(
                  "c_name" -> q1br("c_name"),
                  "p_name" -> pq1r("p_name"),
                  "month" -> cq1br("o_orderdate"),
                  "t_qty" -> pq1r("l_qty")))))))),
      List("c_name", "p_name", "month"),
      List("t_qty")
    )

  // query4 like with filter
  val query4b =
    SumByKey(
      ForeachUnion(q1r, Q1r,
        ForeachUnion(cq1r, BagProject(q1r, "c_orders"),
          ForeachUnion(pq1r, BagProject(cq1r, "o_parts"),
            Singleton(Tuple(
              "c_name" -> q1r("c_name"),
              "p_name" -> pq1r("p_name"),
              "month" -> cq1r("o_orderdate"),
              "t_qty" -> pq1r("l_qty")))))),
      List("c_name", "p_name", "month"),
      List("t_qty")
    )

  val query4 =
    DeDup(ForeachUnion(q1r, Q1r,
      ForeachUnion(cq1r, BagProject(q1r, "c_orders"),
        ForeachUnion(pq1r, BagProject(cq1r, "o_parts"),
          Singleton(Tuple("c_name" -> q1r("c_name"), "p_name" -> pq1r("p_name"), "month" -> cq1r("o_orderdate"),
            "t_qty" ->
              Sum(ForeachUnion(pq2r, BagProject(cq1r, "o_parts"),
                IfThenElse(Cmp(OpEq, pq2r("p_name"), pq1r("p_name")),
                  Singleton(Tuple("l_qty" -> pq2r("l_qty").asInstanceOf[NumericExpr])))),
                List("l_qty")
              )("l_qty")
          ))))))
  // Query 2

  val relS = BagVarRef("S", TPCHSchema.suppliertype)
  val sr = TupleVarRef("s", TPCHSchema.suppliertype.tp)
  
  val query2 = ForeachUnion(sr, relS,
            Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> ForeachUnion(lr, relL,
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")), 
                ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                    ForeachUnion(cr, relC,
                      IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                        Singleton(Tuple("c_name2" -> cr("c_name"))))))))))))


  val relPS = BagVarRef("PS", TPCHSchema.partsupptype)
  val psr = TupleVarRef("ps", TPCHSchema.partsupptype.tp)

  // Query 3
  val q3data = s"""
    |val C = TPCHLoader.loadCustomer[Customer].toList
    |val O = TPCHLoader.loadOrders[Orders].toList
    |val L = TPCHLoader.loadLineitem[Lineitem].toList
    |val P = TPCHLoader.loadPart[Part].toList
    |val PS = TPCHLoader.loadPartSupp[PartSupp].toList
    |val S = TPCHLoader.loadSupplier[Supplier].toList""".stripMargin

  val q3spark = s"""
    |val tpch = TPCHLoader(spark)
    |val C = tpch.loadCustomers
    |C.cache
    |C.count
    |val O = tpch.loadOrders
    |O.cache
    |O.count
    |val L = tpch.loadLineitem
    |L.cache
    |L.count
    |val P = tpch.loadPart
    |P.cache
    |P.count
    |val PS = tpch.loadPartSupp
    |PS.cache
    |PS.count
    |val S = tpch.loadSupplier
    |S.cache
    |S.count""".stripMargin

  val sq3spark = s"""
    |val tpch = TPCHLoader(spark)
    |val C__F = 1
    |val C__D_1 = tpch.loadCustomers()
    |C__D_1.cache
    |C__D_1.count
    |val O__F = 2
    |val O__D_1 = tpch.loadOrders()
    |O__D_1.cache
    |O__D_1.count
    |val L__F = 3
    |val L__D_1 = tpch.loadLineitem()
    |L__D_1.cache
    |L__D_1.count
    |val P__F = 4
    |val P__D_1 = tpch.loadPart()
    |P__D_1.cache
    |P__D_1.count
    |val PS__F = 5
    |val PS__D_1 = tpch.loadPartSupp
    |PS__D_1.cache
    |PS__D_1.count
    |val S__F = 6
    |val S__D_1 = tpch.loadSupplier
    |S__D_1.cache
    |S__D_1.count""".stripMargin

  val sq3data = s"""
    |val C__F = 1
    |val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
    |val O__F = 2
    |val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
    |val L__F = 3
    |val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
    |val P__F = 4
    |val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
    |val PS__F = 5
    |val PS__D = (List((PS__F, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
    |val S__F = 6
    |val S__D = (List((S__F, TPCHLoader.loadSupplier[Supplier].toList)), ())""".stripMargin

  val q3name = "Query3"
  val query3 = ForeachUnion(pr, relP,
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
  
  // Query 5
  val q5name = "Query5"
  val rQ3 = BagVarRef(q5name, query3.tp)
  val rq3 = TupleVarRef("q3", query3.tp.asInstanceOf[BagType].tp)
  val rc5 = TupleVarRef("c", query3.tp.asInstanceOf[BagType].tp.attrTps("customers").asInstanceOf[BagType].tp)
  val rs5 = TupleVarRef("s", query3.tp.asInstanceOf[BagType].tp.attrTps("suppliers").asInstanceOf[BagType].tp)
  
  val sq5data = s"""
    |case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
    |val Query5__F = ShredQuery5._1.head.lbl
    |val Query5__D = (ShredQuery5._2, Input_Q3_Dict1((ShredQuery5._4, Unit), (ShredQuery5._6, Unit)))""".stripMargin

  val sq5spark = s"""
    |val Query5__F = M_ctx1.collect.take(1)(0).lbl
    |val Query5__D_1 = M_flat1.flatMap(r => r._2)
    |Query5__D_1.cache
    |Query5__D_1.count
    |val Query5__D_2suppliers_1 = M_flat2.map(r => (r._1, r._2))
    |Query5__D_2suppliers_1.cache
    |Query5__D_2suppliers_1.count
    |val Query5__D_2customers_1 = M_flat3.map(r => (r._1, r._2))
    |Query5__D_2customers_1.cache
    |Query5__D_2customers_1.count
  """

  val query5 = ForeachUnion(rq3, rQ3,
                Singleton(Tuple("p_name" -> rq3("p_name"), "cnt" -> 
                  Count(ForeachUnion(rc5, BagProject(rq3, "customers"),
                    IfThenElse(Cmp(OpEq, 
                      Count(ForeachUnion(rs5, BagProject(rq3, "suppliers"),
                              IfThenElse(Cmp(OpEq, rc5("c_nationkey"), rs5("s_nationkey")), 
                                Singleton(Tuple("flag" -> Const(true, BoolType)))))),
                      Const(0, IntType)),
                      Singleton(Tuple("p_name" -> rq3("p_name")))))))))

  val q7name = "Query7"
  val Q37r = BagVarRef(q7name, query3.tp)
  val q7data = "val N = TPCHLoader.loadNation[Nation].toList"
  val sq7data = s"""
    |case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
    |val N__F = 7
    |val N__D = (List((N__F, TPCHLoader.loadNation[Nation].toList)), ())
    |val Query7__F = ShredQuery7._1.head.lbl
    |val Query7__D = (ShredQuery7._2, Input_Q3_Dict1((ShredQuery7._4, Unit), (ShredQuery7._6, Unit)))""".stripMargin

  val relN = BagVarRef("N", TPCHSchema.nationtype)
  val nref = TupleVarRef("n", TPCHSchema.nationtype.tp)

  val sref = TupleVarRef("s", TupleType(Map("s_name" -> StringType, "s_nationkey" -> IntType)))

  val suppliersCond1 = ForeachUnion(sref, rq3("suppliers").asInstanceOf[BagExpr],
                        IfThenElse(Cmp(OpEq, sref("s_nationkey"), nref("n_nationkey")),
                                   Singleton(Tuple("count" -> Const(1, IntType)))).asInstanceOf[BagExpr])
  
  val cref = TupleVarRef("c", TupleType(Map("c_name" -> StringType, "c_nationkey" -> IntType)))
  val customersCond1 = ForeachUnion(cref, rq3("customers").asInstanceOf[BagExpr],
                        IfThenElse(Cmp(OpEq, cref("c_nationkey"), nref("n_nationkey")),
                                   Singleton(Tuple("count" -> Const(1, IntType)))).asInstanceOf[BagExpr])

  val query7 =  ForeachUnion(nref, relN,
                  Singleton(Tuple("n_name" -> nref("n_name"),
                                  "part_names" -> ForeachUnion(rq3, Q37r,
                                                    ForeachUnion(sref, rq3("suppliers").asInstanceOf[BagExpr],
                                                      IfThenElse(And(Cmp(OpEq, sref("s_nationkey"), nref("n_nationkey")),
                                                                     Cmp(OpEq, Count(customersCond1), Const(0, IntType))),
                                                                  Singleton(Tuple("p_name" -> rq3("p_name")))))))))

}
