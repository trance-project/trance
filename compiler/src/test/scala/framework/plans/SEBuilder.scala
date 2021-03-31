package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}
import scala.collection.mutable.HashMap
import scala.collection.immutable.{Map => IMap}

class TestSEBuilder extends FunSuite with Materialization with 
  MaterializeNRC with NRCTranslator with Shredding {

  val occur = new Occurrence{}
  val cnum = new CopyNumber{}
  val samps = new Biospecimen{}
  val tbls = Map("Customer" -> TPCHSchema.customertype,
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
    Unnester.unnest(ncalc)(Map(), Map(), None, "_2")
  }

  def getProgPlan(query: Program, shred: Boolean = false): LinearCSet = {

    val program = if (shred){

      val (shredded, shreddedCtx) = shredCtx(query)
      val optShredded = optimize(shredded)
      val materialized = materialize(optShredded, eliminateDomains = true)
      materialized.program

    }else query

    val ncalc = normalizer.finalize(translate(program)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(IMap(), IMap(), None, "_2")).asInstanceOf[LinearCSet]
  
  }

  test("standard program compilation"){

    val query1str = 
      s"""
        cnvCases1 <= 
          for s in samples union 
            for c in copynumber union 
              if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
              then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};

        hybridScore1 <= 
            for o in occurrences union
              {( oid := o.oid, sid := o.donorId, cands := 
                ( for t in o.transcript_consequences union
                    for c in cnvCases1 union
                      if (t.gene_id = c.gene && o.donorId = c.sid) then
                        {( gene := t.gene_id, score1 := (c.cnum + 0.01) * if (t.impact = "HIGH") then 0.80 
                            else if (t.impact = "MODERATE") then 0.50
                            else if (t.impact = "LOW") then 0.30
                            else 0.01 )}).sumBy({gene}, {score1}) )}
      """
    val query1 = parser.parse(query1str).get
    val plan1 = getProgPlan(query1.asInstanceOf[Program])
    println(Printer.quote(plan1))

    val query2str = 
      s"""
        cnvCases2 <= 
          for s in samples union 
            for c in copynumber union 
              if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
              then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};

        hybridScore2 <= 
          for o in occurrences union
            {( oid := o.oid, sid := o.donorId, cands := 
              ( for t in o.transcript_consequences union
                  for c in cnvCases2 union
                    if (t.gene_id = c.gene && o.donorId = c.sid) then
                      {( gene := t.gene_id, score2 := (c.cnum + 0.01) * t.polyphen_score )}).sumBy({gene}, {score2}) )}
      """
    val query2 = parser.parse(query2str).get
    val plan2 = getProgPlan(query2.asInstanceOf[Program])
    println(Printer.quote(plan2))

    val progs = Vector(plan1, plan2).zipWithIndex

    val subexprs = HashMap.empty[(CExpr, Int), Integer]
    progs.foreach(p => SEBuilder.equivSig(p)(subexprs))
    val subs = SEBuilder.sharedSubs(progs, subexprs, false)

    subs.foreach{ p => 
      println(p._1)
      p._2.foreach{
        s => println(Printer.quote(s.subplan))
      }
    }

  }

  // test("shredded compilation"){

  //   val query1str = 
  //     s"""
  //       hybridScore1 <= 
  //           for o in occurrences union
  //             {( oid := o.oid, sid := o.donorId, cands := 
  //               ( for t in o.transcript_consequences union
  //                   for c in copynumber union
  //                     if (t.gene_id = c.cn_gene_id) then
  //                       {( gene := t.gene_id, score1 := (c.cn_copy_number + 0.01) * if (t.impact = "HIGH") then 0.80 
  //                           else if (t.impact = "MODERATE") then 0.50
  //                           else if (t.impact = "LOW") then 0.30
  //                           else 0.01 )}).sumBy({gene}, {score1}) )}
  //     """
  //   val query1 = parser.parse(query1str).get
  //   val plan1 = getProgPlan(query1.asInstanceOf[Program])

  //   val query2str = 
  //     s"""
  //       hybridScore2 <= 
  //         for o in occurrences union
  //           {( oid := o.oid, sid := o.donorId, cands := 
  //             ( for t in o.transcript_consequences union
  //                 for c in copynumber union
  //                   if (t.gene_id = c.cn_gene_id) then
  //                     {( gene := t.gene_id, score2 := (c.cn_copy_number + 0.01) * t.polyphen_score )}).sumBy({gene}, {score2}) )}
  //     """
  //   val query2 = parser.parse(query2str).get
  //   val plan2 = getProgPlan(query2.asInstanceOf[Program])
  //   val splan2 = getProgPlan(query2.asInstanceOf[Program], true)
  //   println(Printer.quote(splan2))


  // }


  // test("input hash"){
  //   val cust1 = InputRef("Customer", TPCHSchema.customertype)
  //   val cust2 = InputRef("Customer2", TPCHSchema.customertype)

  //   val p1 = SEBuilder.signature(cust1)
  //   val p2 = SEBuilder.signature(cust2)
  //   assert(p1 != p2)

  // }

  // test("select hash"){
  //   val c = Variable.fresh(TPCHSchema.customertype.tp)

  //   val cust1 = Select(InputRef("Customer", TPCHSchema.customertype), c, Constant(true), c)
  //   val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
  //   val p1 = SEBuilder.signature(cust1)
  //   val p2 = SEBuilder.signature(cust2)
  //   assert(p1 == p2)

  // }

  // test("project hash"){

  //   val cust1 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
  //   val custPlan1 = getPlan(cust1.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val cust2 = parser.parse("for c in Customer union { (custkey := c.c_custkey ) }", parser.term).get
  //   val custPlan2 = getPlan(cust2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val p1 = SEBuilder.signature(custPlan1)
  //   val p2 = SEBuilder.signature(custPlan2)
  //   assert(p1 == p2)

  //   val ord = parser.parse("for o in Order union { ( odate := o.o_orderdate ) }", parser.term).get
  //   val ordPlan = getPlan(ord.asInstanceOf[Expr]).asInstanceOf[CExpr]
  //   val p3 = SEBuilder.signature(ordPlan)
  //   assert(p1 != p3)
  
  // }

  // test("join hash"){
  //   val joinQuery1 = parser.parse(
  //     """
  //       for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, odate := o.o_orderdate )}
  //     """, parser.term).get
  //   val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val joinQuery2 = parser.parse(
  //     """
  //       for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, odate := o.o_orderdate )}
  //     """, parser.term).get
  //   val joinPlan2 = getPlan(joinQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]
  
  //   val p1 = SEBuilder.signature(joinPlan1)
  //   val p2 = SEBuilder.signature(joinPlan2)
  //   assert(p1 == p2)

  //   // alternative join condition
  //   val joinQuery3 = parser.parse(
  //     """
  //       for o in Order union
  //         for c in Customer union 
  //           if (c.c_name = o.o_orderdate)
  //           then {(cname := c.c_name, odate := o.o_orderdate )}
  //     """, parser.term).get
  //   val joinPlan3 = getPlan(joinQuery3.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val p3 = SEBuilder.signature(joinPlan3)
  //   assert(p2 != p3)

  // }

  // test("unnest hash"){
  //   val unnestQuery1 = parser.parse(
  //     """
  //       for o in occurrences union
  //         for t in o.transcript_consequences union 
  //           {( oid := o.oid, impact := t.impact )}
  //     """, parser.term).get
  //   val unnestPlan1 = getPlan(unnestQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val unnestQuery2 = parser.parse(
  //     """
  //       for o in occurrences union
  //         for t in o.transcript_consequences union 
  //           {( sid := o.donorId, impact := t.impact )}
  //     """, parser.term).get
  //   val unnestPlan2 = getPlan(unnestQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val p1 = SEBuilder.signature(unnestPlan1)
  //   val p2 = SEBuilder.signature(unnestPlan2)
  //   assert(p1 == p2)

  // }

  // test("reduce hash"){
  //   val reduceQuery1 = parser.parse(
  //     """
  //       (for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, orderkey := o.o_orderkey )}).sumBy({cname}, {orderkey})
  //     """, parser.term).get
  //   val reducePlan1 = getPlan(reduceQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val reduceQuery2 = parser.parse(
  //     """
  //       (for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(custkey := c.c_custkey, otherkey := o.o_custkey )}).sumBy({custkey}, {otherkey})
  //     """, parser.term).get
  //   val reducePlan2 = getPlan(reduceQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val p1 = SEBuilder.signature(reducePlan1)
  //   val p2 = SEBuilder.signature(reducePlan2)
  //   assert(p1 == p2)

  // }

  // test("input SEs"){
  //   val cust1 = InputRef("Customer", TPCHSchema.customertype)
  //   val cust2 = InputRef("Customer2", TPCHSchema.customertype)

  //   val p1 = SEBuilder.subexpressions(cust1)
  //   val p2 = SEBuilder.subexpressions(cust2)
  //   assert(p1.size == 1)
  //   assert(p2.size == 1)
  //   assert(p1 != p2)

  // }
  
  // test("select SEs"){

  //   val cust1 = InputRef("Customer", TPCHSchema.customertype)
  //   val p1 = SEBuilder.subexpressions(cust1)

  //   val c = Variable.fresh(TPCHSchema.customertype.tp)

  //   val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c, Gt(CProject(c, "custkey"), Constant(20)), c)
  //   val cust3 = Select(InputRef("Customer", TPCHSchema.customertype), c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
  //   val p2 = SEBuilder.subexpressions(cust2)
  //   val p3 = SEBuilder.subexpressions(cust3)

  //   // should be equal up to top-level plan
  //   assert((p2.values.toSet & p3.values.toSet).nonEmpty)

  //   // should contain one matching subplan
  //   assert((p2 filterKeys p1.keySet).values.toSet == p1.values.toSet)
  //   assert((p3 filterKeys p1.keySet).values.toSet == p1.values.toSet)

  //   val plans = Vector(cust1.asInstanceOf[CExpr], 
  //     cust2.asInstanceOf[CExpr], cust3.asInstanceOf[CExpr]).zipWithIndex
  //   val subexprs = HashMap.empty[(CExpr, Int), Integer]
  //   plans.foreach(p => SEBuilder.equivSig(p)(subexprs))

  //   val subs = SEBuilder.sharedSubs(plans, subexprs)
  //   // shares inputs and shares filters
  //   assert(subs.size == 2)
  //   val check1 = subs.filter(x => x._2.filter(y => y.wid == 0).nonEmpty).head._2.toSet
  //   val check2 = subs.filter(x => x._2.filter(y => y.wid != 0).nonEmpty).head._2.toSet
  //   assert(check1 == Set(cust1))
  //   assert(check2 == Set(cust2, cust3))

  // }

  // test("join SEs"){

  //   val cust = AddIndex(InputRef("Customer", TPCHSchema.customertype), "Customer_index")
  //   val c = Variable.fresh(cust.tp)

  //   val cust1 = Select(cust, c, Gt(CProject(c, "custkey"), Constant(20)), c)
  //   val cust2 = Select(cust, c, Lt(CProject(c, "custkey"), Constant(2)), c)
    
  //   val p1 = SEBuilder.subexpressions(cust1)
  //   val p2 = SEBuilder.subexpressions(cust2)
   
  //   val joinQuery1 = parser.parse(
  //     """
  //       for c in Customer union 
  //         if (c.c_custkey > 25)
  //         then for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, odate := o.o_orderdate )}
  //     """, parser.term).get
  //   val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
  //   val p3 = SEBuilder.subexpressions(joinPlan1)

  //   assert((p1.values.toSet & p3.values.toSet).nonEmpty)

  //   val plans = Vector(cust1, cust2, joinPlan1).zipWithIndex
  //   val subexprs = HashMap.empty[(CExpr, Int), Integer]
  //   plans.foreach(p => SEBuilder.equivSig(p)(subexprs))

  //   val subs = SEBuilder.sharedSubs(plans, subexprs)
  //   // input, index, selects
  //   assert(subs.size == 3)
  //   for (k <- p2.values){
  //     assert(subs(k).size == 3) 
  //   }

  // } 

  // test("two joins SEs"){
  //   val joinQuery1 = parser.parse(
  //     """
  //       for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, orderkey := o.o_orderkey )}
  //     """, parser.term).get
  //   val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val joinQuery2 = parser.parse(
  //     """
  //       for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(custkey := c.c_custkey, otherkey := o.o_custkey )}
  //     """, parser.term).get
  //   val joinPlan2 = getPlan(joinQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val plans = Vector(joinPlan1, joinPlan2).zipWithIndex
  //   val subexprs = HashMap.empty[(CExpr, Int), Integer]
  //   plans.foreach(p => SEBuilder.equivSig(p)(subexprs))

  //   val subs = SEBuilder.sharedSubs(plans, subexprs)
  //   assert(subs.size == 8)

  //   val subsLimit = SEBuilder.sharedSubs(plans, subexprs, true)
  //   assert(subsLimit.size == 1)
  //   assert(subsLimit.head._2.size == 2)
    
  // } 

  // test("reduce SEs"){
  //   val reduceQuery1 = parser.parse(
  //     """
  //       (for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, orderkey := o.o_orderkey )}).sumBy({cname}, {orderkey})
  //     """, parser.term).get
  //   val reducePlan1 = getPlan(reduceQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val reduceQuery2 = parser.parse(
  //     """
  //       (for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(custkey := c.c_custkey, otherkey := o.o_custkey )}).sumBy({custkey}, {otherkey})
  //     """, parser.term).get
  //   val reducePlan2 = getPlan(reduceQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val plans = Vector(reducePlan1, reducePlan2).zipWithIndex
  //   val subexprs = HashMap.empty[(CExpr, Int), Integer]
  //   plans.foreach(p => SEBuilder.equivSig(p)(subexprs))
  //   val subs = SEBuilder.sharedSubs(plans, subexprs)
  //   assert(subs.size == 9)

  //   val subsLimit = SEBuilder.sharedSubs(plans, subexprs, true)
  //   assert(subsLimit.size == 1)
  //   assert(subsLimit.head._2.size == 2)

  // }

  // test("combined SEs"){
  //   val joinQuery1 = parser.parse(
  //     """
  //       for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, orderkey := o.o_orderkey )}
  //     """, parser.term).get
  //   val joinPlan1 = getPlan(joinQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val joinQuery2 = parser.parse(
  //     """
  //       for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(custkey := c.c_custkey, otherkey := o.o_custkey )}
  //     """, parser.term).get
  //   val joinPlan2 = getPlan(joinQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val reduceQuery1 = parser.parse(
  //     """
  //       (for c in Customer union 
  //         for o in Order union
  //           if (c.c_custkey = o.o_custkey)
  //           then {(cname := c.c_name, orderkey := o.o_orderkey )}).sumBy({cname}, {orderkey})
  //     """, parser.term).get
  //   val reducePlan1 = getPlan(reduceQuery1.asInstanceOf[Expr]).asInstanceOf[CExpr]
    
  //   val reduceQuery2 = parser.parse(
  //     """
  //       (for o in Order union
  //         for c in Customer union 
  //           if (c.c_custkey = o.o_custkey)
  //           then {(custkey := c.c_custkey, otherkey := o.o_custkey )}).sumBy({custkey}, {otherkey})
  //     """, parser.term).get
  //   val reducePlan2 = getPlan(reduceQuery2.asInstanceOf[Expr]).asInstanceOf[CExpr]

  //   val plans = Vector(reducePlan1, reducePlan2, joinPlan1, joinPlan2).zipWithIndex
  //   val subexprs = HashMap.empty[(CExpr, Int), Integer]
  //   plans.foreach(p => SEBuilder.equivSig(p)(subexprs))
  //   val subs = SEBuilder.sharedSubs(plans, subexprs)

  //   val subsLimit = SEBuilder.sharedSubs(plans, subexprs, true)
  //   println(subsLimit)
  //   println(subsLimit.size)

  // }

}
