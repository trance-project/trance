package framework.optimize

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.Map
import framework.plans._

trait TestBase extends FunSuite with Materialization with 
  MaterializeNRC with NRCTranslator with Shredding {
  
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

  val inputs = Map(InputRef("occurrences", StringType).vstr-> "occurrences",
                   InputRef("copynumber", StringType).vstr -> "copynumber",
                   InputRef("samples", StringType).vstr -> "samples")

  val parser = Parser(tbls)
  val normalizer = new Finalizer(new BaseNormalizer{})
  val optimizer = new Optimizer()

  def getPlan(query: Expr): CExpr = {
    val compiler = new BaseCompiler{}
    val compile = new Finalizer(compiler)
    val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
    val opt = optimizer.applyPush(Unnester.unnest(ncalc)(IMap(), IMap(), None, "_2"))
    compile.finalize(opt).asInstanceOf[CExpr]
  }

  def getProgPlan(query: Program, shred: Boolean = false): LinearCSet = {

    val program = if (shred){

      val (shredded, shreddedCtx) = shredCtx(query)
      val optShredded = optimize(shredded)
      val materialized = materialize(optShredded, eliminateDomains = true)
      materialized.program

    }else query

    val compiler = if (shred) new ShredOptimizer{} else new BaseCompiler{}
    val compile = new Finalizer(compiler)
    val ncalc = normalizer.finalize(translate(program)).asInstanceOf[CExpr]
    val unopt = Unnester.unnest(ncalc)(IMap(), IMap(), None, "_2")
    val opt = optimizer.applyAll(unopt).asInstanceOf[LinearCSet]
    // pass through another compilation stage
    compile.finalize(opt).asInstanceOf[LinearCSet]
  }

  // val query1str = 
  //   s"""
  //     cnvCases1 <= 
  //       for s in samples union 
  //         for c in copynumber union 
  //           if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
  //           then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};

  //     hybridScore1 <= 
  //         for o in occurrences union
  //           {( oid := o.oid, sid1 := o.donorId, cands1 := 
  //             ( for t in o.transcript_consequences union
  //                if (t.sift_score > 0.0)
  //                then for c in cnvCases1 union
  //                   if (t.gene_id = c.gene && o.donorId = c.sid) then
  //                     {( gene1 := t.gene_id, score1 := (c.cnum + 0.01) * if (t.impact = "HIGH") then 0.80 
  //                         else if (t.impact = "MODERATE") then 0.50
  //                         else if (t.impact = "LOW") then 0.30
  //                         else 0.01 )}).sumBy({gene1}, {score1}) )}
  //   """
  // val query1 = parser.parse(query1str).get.asInstanceOf[Program]
  // val plan1 = getProgPlan(query1)
  // val splan1 = getProgPlan(query1, true)

  // // println(Printer.quote(splan1))

  // val query2str = 
  //   s"""
  //     cnvCases2 <= 
  //       for s in samples union 
  //         for c in copynumber union 
  //           if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
  //           then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};

  //     hybridScore2 <= 
  //       for o in occurrences union
  //         {( oid := o.oid, sid2 := o.donorId, cands2 := 
  //           ( for t in o.transcript_consequences union
  //              if (t.polyphen_score > 0.0)
  //              then for c in cnvCases2 union
  //                 if (t.gene_id = c.gene && o.donorId = c.sid) then
  //                   {( gene2 := t.gene_id, score2 := (c.cnum + 0.01) * t.polyphen_score )}).sumBy({gene2}, {score2}) )}
  //   """
  // val query2 = parser.parse(query2str).get.asInstanceOf[Program]
  // val plan2 = getProgPlan(query2)
  // val splan2 = getProgPlan(query2, true)

  // // println(Printer.quote(splan2))

  // // this will make sure things are being 
  // // considered equivalent
  // val query3str = 
  //   s"""
  //     cnvCases3 <= 
  //       for s in samples union  
  //         {(sid := s.bcr_patient_uuid)};

  //     hybridScore3 <=
  //       for o in occurrences union 
  //         {( oid := o.oid, sid3 := o.donorId, cands3 := 
  //           for t in o.transcript_consequences union 
  //             {( gene3 := t.gene_id, score3 := t.impact )} )}
  //   """

  // val query3 = parser.parse(query3str).get.asInstanceOf[Program]
  // val plan3 = getProgPlan(query3)
  // val splan3 = getProgPlan(query3, true)

  // // println(Printer.quote(splan3))

  // val query4str = 
  //   s"""
  //     samplesProj <= 
  //       for s in samples union 
  //         {( sid := s.bcr_patient_uuid, aid := s.bcr_aliquot_uuid )};

  //     copyProj <= 
  //       for c in copynumber union 
  //         {( caid := c.cn_aliquot_uuid, cgene := c.cn_gene_id, cnum := c.cn_copy_number )};

  //     cnvCases <= 
  //       for s1 in samplesProj union 
  //         for c1 in copyProj union 
  //           if (s1.aid = c1.caid)
  //           then {( id := s1.sid, gene := c1.cgene, num := c1.cnum )}

  //   """

  // val query5str = 
  //   s"""
  //     samplesProj2 <= 
  //       for s in samples union 
  //         {( sid := s.bcr_patient_uuid, aid := s.bcr_aliquot_uuid )};

  //     copyProj2 <= 
  //       for c in copynumber union 
  //         {( caid := c.cn_aliquot_uuid, cgene := c.cn_gene_id, cnum := c.cn_copy_number )};

  //     cnvCases2 <= 
  //       for s1 in samplesProj2 union 
  //         for c1 in copyProj2 union 
  //           if (s1.aid = c1.caid)
  //           then {( id := s1.sid, gene := c1.cgene, num := c1.cnum )}

  //   """

  // val query4 = parser.parse(query4str).get.asInstanceOf[Program]
  // val plan4 = getProgPlan(query4)
  // val splan4 = getProgPlan(query4, true)

  // val query5 = parser.parse(query5str).get.asInstanceOf[Program]
  // val plan5 = getProgPlan(query5)
  // val splan5 = getProgPlan(query5, true)

  val progs = Vector() //Vector(plan1, plan2, plan3).zipWithIndex
  val sprogs = Vector() //Vector(splan1, splan2, splan3).zipWithIndex
  val progsSimple = Vector() //Vector(plan4, plan5).zipWithIndex
  val sprogsSimple = Vector() //Vector(splan4, splan5).zipWithIndex

  // val query6str = 
  //   s"""
  //     Test <= 
  //     for s in samples union 
  //       {( sid := s.bcr_patient_uuid, scores := 
  //         (for o in occurrences union
  //           if (o.donorId = s.bcr_patient_uuid)
  //           then for t in o.transcript_consequences union 
  //             {( gene := t.gene_id, score := t.sift_score )}
  //         ).sumBy({gene}, {score})
  //       )}

  //   """

  // val query6 = parser.parse(query6str).get.asInstanceOf[Program]
  // val plan6 = getProgPlan(query6)
  // val splan6 = getProgPlan(query6, true)

  val query7str = 
    s"""
      Test <= 
      for s in samples union 
        {( sid := s.bcr_patient_uuid, muts := 
          for o in occurrences union
            if (o.donorId = s.bcr_patient_uuid)
            then {( oid := o.oid, scores := 
              (for t in o.transcript_consequences union 
                {( gene := t.gene_id, score := t.sift_score )}
              ).sumBy({gene}, {score})
          )}
        )}

    """

  val query7 = parser.parse(query7str).get.asInstanceOf[Program]
  // val plan7 = getProgPlan(query7)
  

  def printSE(ses: Map[Integer, List[SE]]): Unit = {
    ses.foreach{ s => 
      println(s._1)
      s._2.foreach{ x => println(Printer.quote(x.subplan)) }
      println("")
    }
  } 

  def printCE(ses: IMap[Integer, CNamed]): Unit = {
    ses.foreach{ s => 
      println(s._1)
      println(s._2.vstr)
      println(Printer.quote(s._2))
      println("")
    }
  } 

}