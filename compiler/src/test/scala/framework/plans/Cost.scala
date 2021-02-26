package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}

class TestCost extends FunSuite with MaterializeNRC with NRCTranslator {
  val occur = new Occurrence{}
  val cnum = new CopyNumber{}
  val tbls = Map("Occur" -> BagType(occur.occurmid_type), 
                  "copynumber" -> BagType(cnum.copyNumberType))

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})
  val optimizer = new Optimizer()

  def getPlan(query: Assignment): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(Map(), Map(), None, "_2"))
  }

  test("simple copy number"){
    val cnum1 =  """ 
      TestCnum <=
      for c in copynumber union 
        if (c.cn_copy_number > 0) 
        then {(sid := c.cn_aliquot_uuid, cnum := c.cn_copy_number)}
      """ 
    val query1 = parser.parse(cnum1).get
    val plan1 = getPlan(query1.asInstanceOf[Program].get("TestCnum").get).asInstanceOf[CNamed]
    println("these are the results")

    Cost.runCost(plan1)

  }

}