package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.nrc._

class TestCExpr extends FunSuite with MaterializeNRC with NRCTranslator {

  val tbls = Map("Customer" -> TPCHSchema.customertype,
                 "Order" -> TPCHSchema.orderstype,
                 "Lineitem" -> TPCHSchema.lineittype,
                 "Part" -> TPCHSchema.parttype)

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})

  def getPlan(query: Expr): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    Unnester.unnest(ncalc)(Map(), Map(), None, "_2")
  }

  test("select id"){
    val c = Variable.fresh(TPCHSchema.customertype.tp)
    val customerSelect = Select(InputRef("Customer", TPCHSchema.customertype), c, Constant(true), c)
    assert(customerSelect.label == "Customer" && customerSelect.attributes == Set.empty[String])
  }

  test("project id"){

    val customer = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val customerProject = getPlan(customer.asInstanceOf[Expr]).asInstanceOf[Projection]
    assert(customerProject.label == "Customer")
    assert(customerProject.attributes.isEmpty)

    // same as above test
    val c = Variable.fresh(TPCHSchema.customertype.tp)
    val customerSelect = Select(InputRef("Customer", TPCHSchema.customertype), c, Constant(true), c)
    
    // assert no different from selection 
    assert(customerProject.label == customerSelect.label)
    assert(customerProject.attributes == customerSelect.attributes)

    val customer2 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val customerFilter = getPlan(customer.asInstanceOf[Expr]).asInstanceOf[Projection]
    
    // assert no different than filtering
    assert(customerProject.label == customerFilter.label)
    assert(customerProject.attributes == customerFilter.attributes)
  
  }


}
