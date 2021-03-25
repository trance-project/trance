package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{HashMap, Map}
import framework.plans.{Equals => CEquals, Project => CProject}

class TestCost extends FunSuite with MaterializeNRC with NRCTranslator {

  val compileCost = false

  val occur = new Occurrence{}
  val cnum = new CopyNumber{}
  val samps = new Biospecimen{}
  val tbls = IMap("Customer" -> TPCHSchema.customertype,
                 "Order" -> TPCHSchema.orderstype,
                 "Lineitem" -> TPCHSchema.lineittype,
                 "Part" -> TPCHSchema.parttype, 
                 "occurrences" -> BagType(occur.occurmid_type), 
                 "copynumber" -> BagType(cnum.copyNumberType), 
                 "samples" -> BagType(samps.biospecType))

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})
  val optimizer = new Optimizer()
  
  def getPlan(query: Expr): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(IMap(), IMap(), None, "_2"))
  }

  def getPlan(query: Program): LinearCSet = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(IMap(), IMap(), None, "_2")).asInstanceOf[LinearCSet]
  }

  test("aggregates"){

    val query1str = 
      s"""
        cnvCases1 <= 
          for c in copynumber union
            for s in samples union 
              if (c.cn_aliquot_uuid = s.bcr_aliquot_uuid)
              then {(cn_case_uuid := s.bcr_patient_uuid, cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number)};

        hybridScore1 <= 
          for o in occurrences union
            {( oid := o.oid, sid := o.donorId, cands := 
              ( for t in o.transcript_consequences union
                  for c in cnvCases1 union
                    if (t.gene_id = c.cn_gene_id) then
                      {( gene := t.gene_id, score := (c.cn_copy_number + 0.01) * if (t.impact = "HIGH") then 0.80 
                          else if (t.impact = "MODERATE") then 0.50
                          else if (t.impact = "LOW") then 0.30
                          else 0.01 )}).sumBy({gene}, {score}) )}
      """
    val query1 = parser.parse(query1str).get
    val plan1 = getPlan(query1.asInstanceOf[Program])

    val query2str = 
      s"""
        cnvCases2 <= 
          for c in copynumber union
            for s in samples union 
              if (c.cn_aliquot_uuid = s.bcr_aliquot_uuid)
              then {(cn_case_uuid := s.bcr_patient_uuid, cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number)};

        hybridScore2 <= 
          for o in occurrences union
            {( oid := o.oid, sid := o.donorId, cands := 
              ( for t in o.transcript_consequences union
                  for c in cnvCases2 union
                    if (t.gene_id = c.cn_gene_id) then
                      {( gene := t.gene_id, score := (c.cn_copy_number + 0.01) * t.polyphen_score )}).sumBy({gene}, {score}) )}
      """
    val query2 = parser.parse(query2str).get
    val plan2 = getPlan(query2.asInstanceOf[Program])

    val progs = Vector(plan1, plan2).zipWithIndex
    val plans = progs.flatMap{ case (prog, id) => 
      prog.exprs.map(e => e match {
        case CNamed(name, p) => (p, id)
        case _ => (e, id)
    })}

    val subexprs = HashMap.empty[(CExpr, Int), Integer]
    plans.foreach(p => SEBuilder.equivSig(p)(subexprs))
    val subs = SEBuilder.sharedSubs(plans, subexprs)

    val ces = CEBuilder.buildCoverMap(subs)
    
    val stats = StatsCollector.getCost(subs, ces)
    // assert(stats.size == 25)

    val cost = new Cost(stats)
    // val selected = cost.selectCovers(ces, subs)
    // assert(ces.size == selected.size)


    cost.printEstimateAndStat(ces, subs)

  }

  test("nest test"){
    
    val joinQuery1 = parser.parse(
      """
        for c in Customer union
          if (c.c_name = "test1")
          then for o in Order union 
            if (c.c_custkey = o.o_custkey)
            then {( cname := c.c_name, orderkey := o.o_orderkey )}
      """, parser.term).get
    val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val joinQuery2 = parser.parse(
      """
        for c in Customer union 
          if (c.c_name = "test2")
          then for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {( custkey := c.c_custkey, otherkey := o.o_custkey )}
      """, parser.term).get
    val joinPlan2 = getPlan(joinQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    val nestQuery1 = parser.parse(
      """
        for c in Customer union 
          if (c.c_name = "test1")
          then {(cname := c.c_name, c_orders := for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {( orderkey := o.o_orderkey )})}
      """, parser.term).get
    val nestPlan1 = getPlan(nestQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
    val nestQuery2 = parser.parse(
      """
        for c in Customer union 
          if (c.c_name = "test2")
          then {(custkey := c.c_custkey, n_orders := for o in Order union
            if (c.c_custkey = o.o_custkey)
            then {( otherkey := o.o_custkey )})}
      """, parser.term).get
    val nestPlan2 = getPlan(nestQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

    // first get the fingerprint map
    val plans = Vector(joinPlan1, nestPlan1, joinPlan2, nestPlan2).zipWithIndex

    val subexprs = HashMap.empty[(CExpr, Int), Integer]
    plans.foreach(p => SEBuilder.equivSig(p)(subexprs))
    // println(subexprs.size)
    
    val subs = SEBuilder.sharedSubs(plans, subexprs)
    // println(subs.size)
    val covers = CEBuilder.buildCoverMap(subs)
    // println(covers.size)

    // this will take covers and subs to generate statistics
    val stats = Map.empty[String, Statistics]

    val cost = new Cost(stats)
    val selectedCovers = cost.selectCovers(covers, subs)

    // empty stats will always default to true
    assert(covers.size == selectedCovers.size)

  }

}