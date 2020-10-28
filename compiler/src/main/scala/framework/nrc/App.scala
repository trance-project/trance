package framework.nrc

import framework.common._
import framework.examples.tpch.TPCHSchema
import scala.collection.mutable.HashMap

object App extends MaterializeNRC with Printer {

  def main(args: Array[String]){


    val relC = BagVarRef("Customer", TPCHSchema.customertype)
    val cr = TupleVarRef("c", TPCHSchema.customertype.tp)

    val relO = BagVarRef("Order", TPCHSchema.orderstype)
    val or = TupleVarRef("o", TPCHSchema.orderstype.tp)

    val relL = BagVarRef("Lineitem", TPCHSchema.lineittype)
    val lr = TupleVarRef("l", TPCHSchema.lineittype.tp)

    val relP = BagVarRef("Part", TPCHSchema.parttype)
    val pr = TupleVarRef("p", TPCHSchema.parttype.tp)

    val relS = BagVarRef("Supplier", TPCHSchema.suppliertype)
    val sr = TupleVarRef("s", TPCHSchema.suppliertype.tp)
    
    val relPS = BagVarRef("PartSupp", TPCHSchema.partsupptype)
    val psr = TupleVarRef("ps", TPCHSchema.partsupptype.tp)

    val relN = BagVarRef("Nation", TPCHSchema.nationtype)
    val nr = TupleVarRef("n", TPCHSchema.nationtype.tp)

    val relR = BagVarRef("Region", TPCHSchema.regiontype)
    val rr = TupleVarRef("r", TPCHSchema.regiontype.tp)

    // example of deep embedding?
    // for (c <- relC
    //      yield ("name" -> c.c_name, "orders" -> 
    //         for (o <- relO
    //           yield ("date" -> o.o_date, "parts" ->
    //             for (l <- relL
    //               yield ("qty" -> l.l_qty, "pk" -> l.l_partkey)
    //               )
    //             )
    //           )
    //       )
    //   )

    // start with what i want

    // val query = cr in { relC union ("name" -> cr("c_name")) }
    // println(quote(query))

    // val query1 = cr in { relC union (
    //     "name" -> cr("c_name"), 
    //     "orders" -> { or in { relO union 
    //       ("date" -> or("o_orderdate")) } }
    //   )}


    // println(quote(query1))
    // println(quote(query2))

    val tbls = Map("C" -> TPCHSchema.customertype, 
                   "O" -> TPCHSchema.orderstype,
                   "L" -> TPCHSchema.lineittype)
    val q = 
      s"""
        for c in C union 
          {(name := c.c_name, orders := for o in O union 
              {(date := o.o_orderdate, ok := o.o_orderkey)}
      """

          // )}
      //       for o in O union 
      //         {(date := o.o_orderdate, parts :=
      //             for l in L union 
      //               {(qty := l.l_qty, pk := l.l_partkey)}
      //           )}
      //     )}
      // """
    val parser = Parser(tbls)
    val p = parser.parse(q, parser.term)
    println(quote(p.get.asInstanceOf[Expr]))
        //   for o in O union
        //     if (c.c_custkey == o.o_custkey)
        //     then {(name := c.c_name, date := o.o_orderdate)}

        // """

  }

}


