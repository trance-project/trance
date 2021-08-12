package framework.plans

import scala.collection.mutable._
import play.api.libs.json.Json
import java.io._

import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import scala.collection.immutable.Map
// import framework.plans.{Equals => CEquals, Project => CProject}

object JsonWriter {

	def produceJsonString(plan: CExpr, level: Int = 0): String = plan match {
		case p:Projection => 
			s"""
			|{
			|	"name": "",
			|	"attributes": {
			|		"planOperator": "PROJECT",
			|		"level": $level,
			| 	"newLine": [ "${p.fields.mkString("\",\"")}" ]
			|	},
			|	"children": [${produceJsonString(p.in, level+1)}]
			|}
			""".stripMargin
		case n:Nest =>
			s"""
			|{
			|	"name": "",
			|	"attributes": {
			|		"planOperator": "NEST",
			|		"level": $level,
			| 	"newLine": [ "${n.key.mkString("\",\"")}" ]
			|	},
			|	"children": [${produceJsonString(n.in, level+1)}]
			|}
			""".stripMargin
		case u:UnnestOp =>
			val isOuter = if (u.outer == "outer") "OUTER" else ""
			s"""
			|{
			|	"name": "",
			|	"attributes": {
			|		"planOperator": "${isOuter}UNNEST",
			|		"level": $level,
			| 	"newLine": [ "${u.path}" ]
			|	},
			|	"children": [${produceJsonString(u.in, level+1)}]
			|}
			""".stripMargin
		case j:JoinOp =>
			val isOuter = if (j.jtype == "left_outer") "OUTER" else ""
			s"""
			|{
			|	"name": "",
			|	"attributes": {
			|		"planOperator": "${isOuter}JOIN",
			|		"level": $level,
			| 	"newLine": [ "${Printer.quote(j.cond)}" ]
			|	},
			|	"children": [${produceJsonString(j.left, level+1)},${produceJsonString(j.right, level+1)}]
			|}
			""".stripMargin
		case s:Select =>
			s"""
			|{
			|	"name": "",
			|	"attributes": {
			|		"level": $level,
			| 	"newLine": [ "${Printer.quote(s.p)}" ]
			|	},
			|	"children": [${produceJsonString(s.p, level+1)}]
			|}
			""".stripMargin
		case i:AddIndex => produceJsonString(i.e, level) //TODO pass through for now
		case c:CNamed => produceJsonString(c.e, level) //TODO pass through for now
		case p:LinearCSet => s"""[${p.exprs.map(x => produceJsonString(x)).mkString(",")}]"""
		case _ => s"""{"todo": ${Printer.quote(plan)}}"""
	}

}

object JsonWriterTest extends App with MaterializeNRC with NRCTranslator {

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

	def getPlan(query: Program): LinearCSet = {
		val ncalc = normalizer.finalize(translate(query)).asInstanceOf[CExpr]
		optimizer.applyPush(Unnester.unnest(ncalc)(Map(), Map(), None, "_2")).asInstanceOf[LinearCSet]
	}

	val queryComplicate = 
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

    // this should be 			                    
    // if (t.gene_id = c.cn_gene_id && c.cn_aliquot_uuid = s.bcr_aliquot_uuid) then
    // but i need to fix the parser 
    val querySimple = 
    	s"""
    	  QuerySimple <=
    	  for s in samples union 
   			{(sample := s.bcr_patient_uuid, mutations := 
           		for o in occurrences union
           			if (s.bcr_patient_uuid = o.donorId) then
            		  {( mutId := o.oid, scores := 
              			( for t in o.transcript_consequences union
			                  for c in copynumber union
			                    if (t.gene_id = c.cn_gene_id && c.cn_aliquot_uuid = s.bcr_aliquot_uuid) then
			                      {( gene := t.gene_id, score := (c.cn_copy_number + 0.01) * if (t.impact = "HIGH") then 0.80 
			                          else if (t.impact = "MODERATE") then 0.50
			                          else if (t.impact = "LOW") then 0.30
			                          else 0.01 )}).sumBy({gene}, {score}) )} )}
      """
    // val query1 = parser.parse(querySimple).get
    // val plan1 = getPlan(query1.asInstanceOf[Program])

	// val jsonRep = JsonWriter.getJsonString(plan1)

    val soSimple = 
      s"""
        ShredTest <= for o in occurrences union {(sid := o.donorId, cons := for t in o.transcript_consequences union {(gene := t.gene_id)})}
      """

    val query2 = parser.parse(soSimple).get
    val plan2 = getPlan(query2.asInstanceOf[Program])

	val jsonRep2 = JsonWriter.produceJsonString(plan2)
	println(jsonRep2)

    val jsValue = Json parse jsonRep2
    val pj = Json prettyPrint jsValue
    println(pj)

	// val printer = new PrintWriter(new FileOutputStream(new File("test.json"), false))
 //    printer.println(jsonRep)
 //    printer.close

 //    println(JsonWriter.produceJsonString(plan1))

}