package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}

class TestCEBuilder extends FunSuite with MaterializeNRC with NRCTranslator {

  val occur = new Occurrence{}
  val tbls = Map("Customer" -> TPCHSchema.customertype,
                 "Order" -> TPCHSchema.orderstype,
                 "Lineitem" -> TPCHSchema.lineittype,
                 "Part" -> TPCHSchema.parttype, 
                 "Occur" -> BagType(occur.occurmid_type))

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})
  val optimizer = new Optimizer()

  def getPlan(query: Expr): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(Map(), Map(), None, "_2"))
  }

  test("input SEs"){
    val cust1 = InputRef("Customer", TPCHSchema.customertype)
    val cust2 = InputRef("Customer", TPCHSchema.customertype)

    val ce1 = CEBuilder.buildCover(cust1, cust2)
    assert(ce1 == cust1)

    val ce2 = CEBuilder.buildCover(List(cust1, cust2))
    assert(ce1 == ce2)

  }

  test("indexed SEs"){
    val cust1 = AddIndex(InputRef("Customer", TPCHSchema.customertype), "cust")
    val cust2 = AddIndex(InputRef("Customer", TPCHSchema.customertype), "cust")

    val ce1 = CEBuilder.buildCover(cust1, cust2)
    assert(ce1 == cust1)

    val ce2 = CEBuilder.buildCover(List(cust1, cust2))
    assert(ce1 == ce2)

  }

  test("select covering"){
    val c1 = Variable.fresh(TPCHSchema.customertype.tp)
    val c2 = Variable.fresh(TPCHSchema.customertype.tp)

    val cust1 = Select(InputRef("Customer", TPCHSchema.customertype), c1, Gt(CProject(c1, "custkey"), Constant(20)), c2)
    val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c1, Lt(CProject(c2, "custkey"), Constant(2)), c2)
    
    val ce1 = CEBuilder.buildCover(cust1, cust2).asInstanceOf[Select]
    assert(ce1.p.vstr == "custkey>20||custkey<2")

    val ce2 = CEBuilder.buildCover(List(cust1, cust2))
    assert(ce1.vstr == ce2.vstr)

  }

  test("project covering"){

    val cust1 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val custPlan1 = getPlan(cust1.asInstanceOf[Expr]).asInstanceOf[Projection]

    val cust2 = parser.parse("for c in Customer union { (custkey := c.c_custkey ) }", parser.term).get
    val custPlan2 = getPlan(cust2.asInstanceOf[Expr]).asInstanceOf[Projection]
    assert(custPlan1.fields.toSet == Set("c_name"))
    assert(custPlan2.fields.toSet == Set("c_custkey"))

    val ce1 = CEBuilder.buildCover(custPlan1, custPlan2).asInstanceOf[Projection]
    assert(ce1.fields.toSet == Set("c_custkey", "c_name"))

    val ce2 = CEBuilder.buildCover(List(custPlan1, custPlan2))
    assert(ce1.vstr == ce2.vstr)

  }

}