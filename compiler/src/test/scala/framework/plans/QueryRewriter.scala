package framework.plans

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}
import java.util.UUID.randomUUID

class TestQueryRewriter extends FunSuite with MaterializeNRC with NRCTranslator {

  val occur = new Occurrence{}
  val copynumber = new CopyNumber{}
  val tbls = Map("Customer" -> TPCHSchema.customertype,
                 "Order" -> TPCHSchema.orderstype,
                 "Lineitem" -> TPCHSchema.lineittype,
                 "Part" -> TPCHSchema.parttype, 
                 "Occur" -> BagType(occur.occurmid_type),
                 "CopyNumber" -> BagType(copynumber.copyNumberType))

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})
  val optimizer = new Optimizer()

  def getPlan(query: Expr): CExpr = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(Map(), Map(), None, "_2"))
  }

  def getPlan(query: Program): LinearCSet = {
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    optimizer.applyPush(Unnester.unnest(ncalc)(Map(), Map(), None, "_2")).asInstanceOf[LinearCSet]
  }


  test("select covering"){
    val c1 = Variable.fresh(TPCHSchema.customertype.tp)
    val c2 = Variable.fresh(TPCHSchema.customertype.tp)

    val cust1 = Select(InputRef("Customer", TPCHSchema.customertype), c1, Gt(CProject(c1, "custkey"), Constant(20)), c2)
    val cust2 = Select(InputRef("Customer", TPCHSchema.customertype), c1, Lt(CProject(c2, "custkey"), Constant(2)), c2)
    
    val plan1 = LinearCSet(List(CNamed("Query1", cust1)))
    val plan2 = LinearCSet(List(CNamed("Query2", cust2)))

    val subs = SEBuilder.sharedSubsFromProgram(Vector(plan1, plan2))

    val ces = CEBuilder.buildCovers(subs)

    val newplans = ces.map{
      case c => QueryRewriter.rewritePlans(c)
    }

    // for(c <- newplans){
    //   println(c.cover)
    //   for (s <- c.ses){
    //     println(s)
    //   }
    // }

  }

  test("project covering"){
    val c1 = Variable.fresh(TPCHSchema.customertype.tp)
    val c2 = Variable.fresh(TPCHSchema.customertype.tp)

    val cust1 = parser.parse("for c in Customer union { (cname := c.c_name ) }", parser.term).get
    val custPlan1 = getPlan(cust1.asInstanceOf[Expr]).asInstanceOf[Projection]

    val cust2 = parser.parse("for c in Customer union { (ckey := c.c_custkey ) }", parser.term).get
    val custPlan2 = getPlan(cust2.asInstanceOf[Expr]).asInstanceOf[Projection]
    
    val plan1 = LinearCSet(List(CNamed("Query1", custPlan1)))
    val plan2 = LinearCSet(List(CNamed("Query2", custPlan2)))

    val subs = SEBuilder.sharedSubsFromProgram(Vector(plan1, plan2))

    val ces = CEBuilder.buildCovers(subs)

    val newplans = ces.map{
      case c => QueryRewriter.rewritePlans(c)
    }

    // for(c <- newplans){
    //   println("cover")
    //   println(Printer.quote(c.cover))
    //   for (s <- c.ses){
    //     println("and sub")
    //     println(Printer.quote(s.subplan))
    //   }
    // }

  }

  test("flat aggregates"){

    val query1str = 
      s"""
        Query1 <=
        (for c in CopyNumber union
          {(sid := c.cn_aliquot_uuid, 
            gid := c.cn_gene_id, 
            cnum := c.cn_copy_number)}).sumBy({sid, gid}, {cnum})
      """

    val query1 = parser.parse(query1str).get
    val plan1 = getPlan(query1.asInstanceOf[Program])

    val query2str = 
      s"""
        Query2 <=
        (for c in CopyNumber union
          {(sid := c.cn_aliquot_uuid, 
            gid := c.cn_gene_id, 
            cmax := c.max_copy_number)}).sumBy({sid, gid}, {cmax})
      """

    val query2 = parser.parse(query2str).get
    val plan2 = getPlan(query2.asInstanceOf[Program])

    val query3str = 
      s"""
        Query3 <=
        (for c in CopyNumber union
          {(sid := c.cn_aliquot_uuid, 
            cmax := c.max_copy_number)}).sumBy({sid}, {cmax})
      """

    val query3 = parser.parse(query3str).get
    val plan3 = getPlan(query3.asInstanceOf[Program])

    // equivsig -> {SE}
    val subs = SEBuilder.sharedSubsFromProgram(Vector(plan1, plan2, plan3))

    val ces = CEBuilder.buildCovers(subs)

    val newplans = ces.map{
      case c => QueryRewriter.rewritePlans(c)
    }

    // for(c <- newplans){
    //   println("cover")
    //   println(Printer.quote(c.cover))
    //   for (s <- c.ses){
    //     println("and sub")
    //     println(Printer.quote(s.subplan))
    //   }
    // }
  }

  test("unnest rewrites"){
    val unnestQuery1 = parser.parse(
      """
        Query1 <= 
        for o in Occur union
          if (o.donorId = "fakeTest")
          then for t in o.transcript_consequences union
            if (t.gene_id = "geneA") 
            then {( oid := o.oid, impact := t.impact )}
      """).get
    val plan1 = getPlan(unnestQuery1.asInstanceOf[Program])
    
    val unnestQuery2 = parser.parse(
      """
        Query2 <= 
        for o in Occur union
          if (o.oid = "test")
          then for t in o.transcript_consequences union 
            if (t.sift_score > 0.01)
            then {( sid := o.donorId, poly := t.polyphen_score )}
      """).get
    val plan2 = getPlan(unnestQuery2.asInstanceOf[Program])

    val subs = SEBuilder.sharedSubsFromProgram(Vector(plan1, plan2))

    val ces = CEBuilder.buildCovers(subs)

    val newplans = ces.map{
      case c => QueryRewriter.rewritePlans(c)
    }

    for(c <- newplans){
      println("cover")
      println(Printer.quote(c.cover))
      for (s <- c.ses){
        println("and sub")
        println(Printer.quote(s.subplan))
      }
    }

  }

}
