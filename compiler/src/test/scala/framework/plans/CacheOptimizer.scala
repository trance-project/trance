package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}

class TestCacheOptimizer extends FunSuite with MaterializeNRC with NRCTranslator {

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

    val p1 = CacheOptimizer.equiv(cust1)
    val p2 = CacheOptimizer.equiv(cust2)
    assert(p1 != p2)

  }

  test("select hash"){
    val c = Variable.fresh(TPCHSchema.customertype.tp)

    val cust1 = Select(InputRef("Customer", TPCHSchema.customertype), c, Constant(true), c)
    val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
    val p1 = CacheOptimizer.equiv(cust1)
    val p2 = CacheOptimizer.equiv(cust2)
    assert(p1 == p2)

  }

  test("project hash"){

    val cust1 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val custPlan1 = getPlan(cust1.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val cust2 = parser.parse("for c in Customer union { (custkey := c.c_custkey ) }", parser.term).get
    val custPlan2 = getPlan(cust2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val p1 = CacheOptimizer.equiv(custPlan1)
    val p2 = CacheOptimizer.equiv(custPlan2)
    assert(p1 == p2)

    val ord = parser.parse("for o in Order union { ( odate := o.o_orderdate ) }", parser.term).get
    val ordPlan = getPlan(ord.asInstanceOf[Expr]).asInstanceOf[CExpr]
    val p3 = CacheOptimizer.equiv(ordPlan)
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
  
    val p1 = CacheOptimizer.equiv(joinPlan1)
    val p2 = CacheOptimizer.equiv(joinPlan2)
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
    
    val p3 = CacheOptimizer.equiv(joinPlan3)
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

    val p1 = CacheOptimizer.equiv(unnestPlan1)
    val p2 = CacheOptimizer.equiv(unnestPlan2)
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

    val p1 = CacheOptimizer.equiv(reducePlan1)
    val p2 = CacheOptimizer.equiv(reducePlan2)
    assert(p1 == p2)

  }

}
