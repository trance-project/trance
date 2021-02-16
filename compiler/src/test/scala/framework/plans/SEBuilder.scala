package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}

class TestSEBuilder extends FunSuite with MaterializeNRC with NRCTranslator {

  val occur = new Occurrence{}
  val tbls = Map("Customer" -> TPCHSchema.customertype,
                 "Order" -> TPCHSchema.orderstype,
                 "Lineitem" -> TPCHSchema.lineittype,
                 "Part" -> TPCHSchema.parttype, 
                 "Occur" -> BagType(occur.occurmid_type))

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})

  def getPlan(query: Expr): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    Unnester.unnest(ncalc)(Map(), Map(), None, "_2")
  }

  test("input hash"){
    val cust1 = InputRef("Customer", TPCHSchema.customertype)
    val cust2 = InputRef("Customer2", TPCHSchema.customertype)

    val p1 = SEBuilder.signature(cust1)
    val p2 = SEBuilder.signature(cust2)
    assert(p1 != p2)

  }

  test("select hash"){
    val c = Variable.fresh(TPCHSchema.customertype.tp)

    val cust1 = Select(InputRef("Customer", TPCHSchema.customertype), c, Constant(true), c)
    val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
    val p1 = SEBuilder.signature(cust1)
    val p2 = SEBuilder.signature(cust2)
    assert(p1 == p2)

  }

  test("project hash"){

    val cust1 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val custPlan1 = getPlan(cust1.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val cust2 = parser.parse("for c in Customer union { (custkey := c.c_custkey ) }", parser.term).get
    val custPlan2 = getPlan(cust2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val p1 = SEBuilder.signature(custPlan1)
    val p2 = SEBuilder.signature(custPlan2)
    assert(p1 == p2)

    val ord = parser.parse("for o in Order union { ( odate := o.o_orderdate ) }", parser.term).get
    val ordPlan = getPlan(ord.asInstanceOf[Expr]).asInstanceOf[CExpr]
    val p3 = SEBuilder.signature(ordPlan)
    assert(p1 != p3)
  
  }

  test("join hash"){
    val joinQuery1 = parser.parse(
      """
        for c in Customer union 
          for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {(cname := c.c_name, odate := o.o_orderdate )}
      """, parser.term).get
    val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val joinQuery2 = parser.parse(
      """
        for o in Order union
          for c in Customer union 
            if (c.c_custkey = o.o_custkey)
            then {(cname := c.c_name, odate := o.o_orderdate )}
      """, parser.term).get
    val joinPlan2 = getPlan(joinQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]
  
    val p1 = SEBuilder.signature(joinPlan1)
    val p2 = SEBuilder.signature(joinPlan2)
    assert(p1 == p2)

    // alternative join condition
    val joinQuery3 = parser.parse(
      """
        for o in Order union
          for c in Customer union 
            if (c.c_name = o.o_orderdate)
            then {(cname := c.c_name, odate := o.o_orderdate )}
      """, parser.term).get
    val joinPlan3 = getPlan(joinQuery3.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val p3 = SEBuilder.signature(joinPlan3)
    assert(p2 != p3)

  }

  test("unnest hash"){
    val unnestQuery1 = parser.parse(
      """
        for o in Occur union
          for t in o.transcript_consequences union 
            {( oid := o.oid, impact := t.impact )}
      """, parser.term).get
    val unnestPlan1 = getPlan(unnestQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val unnestQuery2 = parser.parse(
      """
        for o in Occur union
          for t in o.transcript_consequences union 
            {( sid := o.donorId, impact := t.impact )}
      """, parser.term).get
    val unnestPlan2 = getPlan(unnestQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val p1 = SEBuilder.signature(unnestPlan1)
    val p2 = SEBuilder.signature(unnestPlan2)
    assert(p1 == p2)

  }

  test("reduce hash"){
    val reduceQuery1 = parser.parse(
      """
        (for c in Customer union 
          for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {(cname := c.c_name, orderkey := o.o_orderkey )}).sumBy({cname}, {orderkey})
      """, parser.term).get
    val reducePlan1 = getPlan(reduceQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val reduceQuery2 = parser.parse(
      """
        (for o in Order union
          for c in Customer union 
            if (c.c_custkey = o.o_custkey)
            then {(custkey := c.c_custkey, otherkey := o.o_custkey )}).sumBy({custkey}, {otherkey})
      """, parser.term).get
    val reducePlan2 = getPlan(reduceQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val p1 = SEBuilder.signature(reducePlan1)
    val p2 = SEBuilder.signature(reducePlan2)
    assert(p1 == p2)

  }

  test("input SEs"){
    val cust1 = InputRef("Customer", TPCHSchema.customertype)
    val cust2 = InputRef("Customer2", TPCHSchema.customertype)

    val p1 = SEBuilder.subexpressions(cust1)
    val p2 = SEBuilder.subexpressions(cust2)
    assert(p1.size == 1)
    assert(p2.size == 1)
    assert(p1 != p2)

  }
  
  test("select SEs"){

    val cust1 = InputRef("Customer", TPCHSchema.customertype)
    val p1 = SEBuilder.subexpressions(cust1)

    val c = Variable.fresh(TPCHSchema.customertype.tp)

    val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c, Gt(CProject(c, "custkey"), Constant(20)), c)
    val cust3 = Select(InputRef("Customer", TPCHSchema.customertype), c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
    val p2 = SEBuilder.subexpressions(cust2)
    val p3 = SEBuilder.subexpressions(cust3)

    // should be equal up to top-level plan
    assert((p2.values.toSet & p3.values.toSet).nonEmpty)

    // should contain one matching subplan
    assert((p2 filterKeys p1.keySet).values.toSet == p1.values.toSet)
    assert((p3 filterKeys p1.keySet).values.toSet == p1.values.toSet)

    val subs = SEBuilder.sharedSubs(Vector(cust1, cust2, cust3))
    // shares inputs and shares filters
    assert(subs.size == 2)
    assert(subs.head._2.toSet == Set(cust1))
    assert(subs.last._2.toSet == Set(cust2, cust3))

  }

  test("join SEs"){

    val cust = AddIndex(InputRef("Customer", TPCHSchema.customertype), "Customer_index")
    val c = Variable.fresh(cust.tp)

    val cust1 = Select(cust, c, Gt(CProject(c, "custkey"), Constant(20)), c)
    val cust2 = Select(cust, c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
    val p1 = SEBuilder.subexpressions(cust1)
    val p2 = SEBuilder.subexpressions(cust2)
   
    val joinQuery1 = parser.parse(
      """
        for c in Customer union 
          if (c.c_custkey > 25)
          then for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {(cname := c.c_name, odate := o.o_orderdate )}
      """, parser.term).get
    val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    val p3 = SEBuilder.subexpressions(joinPlan1)

    assert((p1.values.toSet & p3.values.toSet).nonEmpty)

    val subs = SEBuilder.sharedSubs(Vector(cust1, cust2, joinPlan1))
    // input, index, selects
    assert(subs.size == 3)
    for (k <- p2.values){
      assert(subs(k).size == 3) 
    }

  }  

  test("reduce SEs"){
    val reduceQuery1 = parser.parse(
      """
        (for c in Customer union 
          for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {(cname := c.c_name, orderkey := o.o_orderkey )}).sumBy({cname}, {orderkey})
      """, parser.term).get
    val reducePlan1 = getPlan(reduceQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val reduceQuery2 = parser.parse(
      """
        (for o in Order union
          for c in Customer union 
            if (c.c_custkey = o.o_custkey)
            then {(custkey := c.c_custkey, otherkey := o.o_custkey )}).sumBy({custkey}, {otherkey})
      """, parser.term).get
    val reducePlan2 = getPlan(reduceQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val subs = SEBuilder.sharedSubs(Vector(reducePlan1, reducePlan2))
    assert(subs.size == 8)

  }

}
