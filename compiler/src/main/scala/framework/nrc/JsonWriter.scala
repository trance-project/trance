package framework.nrc

import scala.collection.mutable._
import java.io._
import play.api.libs.json.Json

import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import scala.collection.immutable.Map
// import framework.plans.{Equals => CEquals, Project => CProject}

trait JsonBasedPrinter extends Printer {
  this: MaterializeNRC =>

  def ind(in: String, n: Int = 0): String = s" $in "
 
  override def quote(e: Expr): String = e match {
    // case c: Const if c.tp == StringType => c.v
    //   // "\"" + c.v + "\""
    case c: Const =>
      c.v.toString
  case Udf(n, e1, tp) => s"$n(${quote(e1)})"
  case v: VarRef =>
      v.name
    case p: Project =>
      quote(p.tuple) + "." + p.field
    case ForeachUnion(x, e1, e2) =>
      s"""|For ${x.name} in ${quote(e1)} Union
          |${ind(quote(e2))}""".stripMargin
    case Union(e1, e2) =>
      s"""|(${quote(e1)})
          |Union
          |(${quote(e2)})""".stripMargin
    case Singleton(e1) => s"""", "labels": [${quote(e1)}] """
    case DeDup(e1) => s"DeDup(${quote(e1)}"
    case Get(e1) => s"Get(${quote(e1)})"
    case Tuple(fs) => 
      val lbls = fs.map(f => f._2.tp match {
        case _:BagType => s"""{ "name": "${f._1}", "key": "${quote(f._2)} }"""
        case _ => s"""{ "name": "${f._1}", "key": "${quote(f._2)}" }"""
      })
      s"""${lbls.mkString(",\n")}"""
      //s"( ${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(",\n   ")} )"
    case l: Let => l.e1 match {
      case _:Tuple => 
        s"""|Let ${l.x.name} =
            |${ind(super.quote(l.e1))} 
            |In ${quote(l.e2)}""".stripMargin
      case _ => 
        s"""|Let ${l.x.name} =
            |${ind(quote(l.e1))} 
            |In ${quote(l.e2)}""".stripMargin
    }
    case c: Cmp =>
      s"${quote(c.e1)} ${c.op} ${quote(c.e2)}"
    case And(e1, e2) =>
      s"${quote(e1)} AND ${quote(e2)}"
    case Or(e1, e2) =>
      s"${quote(e1)} OR ${quote(e2)}"
    case Not(e1) =>
      s"NOT ${quote(e1)}"
    case i: IfThenElse =>
      if (i.e2.isDefined)
        s"""|If (${quote(i.cond)}) Then
            |${ind(quote(i.e1))}
            |Else
            |${ind(quote(i.e2.get))}""".stripMargin
      else
        s"""|If (${quote(i.cond)}) Then
            |${ind(quote(i.e1))}""".stripMargin
    case ArithmeticExpr(op, e1, e2) =>
      s"(${quote(e1)} $op ${quote(e2)})"
    case Count(e1) => s"Count(${quote(e1)})"
    case Sum(e1, fs) =>
      s"Sum(${quote(e1)}, (${fs.mkString(", ")}))"
    // case GroupByKey(e, ks, vs, _) =>
    //   s"""|GroupByKey([${ks.mkString(", ")}], [${vs.mkString(", ")}],
    //       |${ind(quote(e))}
    //       |)""".stripMargin
    // case ReduceByKey(e, ks, vs) =>
    //   s"""|ReduceByKey[${ks.mkString(", ")}], [${vs.mkString(", ")}],
    //       |${ind(quote(e))}
    //       |""".stripMargin
    case GroupByKey(e, ks, vs, _) =>
      s"""|GroupByKey([${ks.mkString(", ")}], [${vs.mkString(", ")}],
          |${ind(quote(e))}
          |)""".stripMargin
    case ReduceByKey(e, ks, vs) =>
      s"""|SumByKey[${ks.mkString(", ")}], [${vs.mkString(", ")}],
          |${ind(quote(e))}
          |""".stripMargin
    // Label extensions
    case x: ExtractLabel =>
//      val tuple = x.lbl.tp.attrTps.keys.mkString(", ")
      val tuple = x.lbl.tp.attrTps.map(x => x._1 + " : " + quote(x._2)).mkString(", ")
      s"""|Extract ${quote(x.lbl)} as ($tuple) In
          |${quote(x.e)}""".stripMargin
    case l: NewLabel =>
      val ps = l.params.map { case (n, p) => n + " := " + quote(p.e) }.toList
//      val ps = l.params.map { case (n, p) => n + " := " + quote(p.e) + " : " + quote(p.tp)}.toList
      s"NewLabel(${(l.id :: ps).mkString(", ")})"

    // Dictionary extensions
    case EmptyDict =>
      "Nil"
    case BagDict(tp, flat, dict) =>
      val params = tp.attrTps.keys.mkString(", ")
//      val params = tp.attrTps.map(x => x._1 + ": " + quote(x._2)).mkString(", ")
      s"""|(($params) ->
          |  flat :=
          |${ind(quote(flat), 2)},
          |  tupleDict :=
          |${ind(quote(dict), 2)}
          |)""".stripMargin
    case TupleDict(fs) =>
      s"( ${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(",\n   ")} )"
    case TupleDictProject(v) => quote(v) + ".tupleDict"
    case d: DictUnion =>
      s"""|(${quote(d.dict1)})
          |DictUnion
          |(${quote(d.dict2)})""".stripMargin

    // Shredding extensions
    case ShredUnion(e1, e2) =>
      s"""|(${quote(e1)})
          |ShredUnion
          |(${quote(e2)})""".stripMargin
    case Lookup(lbl, dict) =>
      s"Lookup(${quote(lbl)}, ${quote(dict)})"

    case KeyValueMapLookup(lbl, dict) =>
      s"KeyValueMapLookup(${quote(lbl)}, ${quote(dict)})"

    case BagToKeyValueMap(b) => quote(b)
    case KeyValueMapToBag(d) => quote(d)

    case _ =>
      sys.error("Cannot print unknown expression " + e)
  }


}

object JsonWriter extends MaterializeNRC with JsonBasedPrinter {

  // obviously support for more types here...
  def produceJsonString(tp: Type): String = tp match {
    case BagType(t) => s"[${produceJsonString(t)}]"
    case TupleType(fs) => 
      val conts = fs.map(f => s""""${f._1}": ${produceJsonString(f._2)} """).mkString(",")
      s"""{$conts}"""
    case _ => 
      s""" "${tp.toString()}" """
  }

  def produceJsonString(query: Assignment): String = {
    val exp = query.rhs match {
      case _:BagToKeyValueMap => s""" "${quote(query.rhs)} }"""
      case _:DeDup => s""" "${quote(query.rhs)})" """
      case qr => qr.tp match {
        case _:BagType => s""" "${quote(query.rhs)} }"""
        case _ => s""" "${quote(query.rhs)}" """
      }
    }
    s"""{"name": "${query.name}", "key": $exp """
  }

  def produceJsonString(query: Program): String = {
    s"""[${query.statements.map(e => produceJsonString(e)).mkString(",")}]"""
  }

  def produceJsonString(query: VarDef): String = 
    s"""{ "name": ${query.name}, "type": ${produceJsonString(query.tp)} }"""

  def produceJsonString(query: Expr): String = quote(query)

}

object JsonWriterTest extends App with Printer with Materialization with MaterializeNRC with Shredding {

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

  def parseProgram(query: String, shred: Boolean = false): String = {

    // parse the input query string
    val program = parser.parse(query).get.asInstanceOf[JsonWriterTest.Program]

    // shred if necessary
    val compiled = if (shred){
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      val materializedProgram = materialize(optShredded, eliminateDomains = true)
      materializedProgram.program
    }else program

    println(quote(compiled))
    // quote(compiled)
    
    // use the json writer from framework.nrc to write 'er
    JsonWriter.produceJsonString(compiled.asInstanceOf[JsonWriter.Program])

  }

  val queryComplicate = 
      s"""
        cnvCases1 <= 
          for c in copynumber union
            for s in samples union 
              if (c.cn_aliquot_uuid == s.bcr_aliquot_uuid)
              then {(cn_case_uuid := s.bcr_patient_uuid, cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number)};

        hybridScore1 <= 
          for o in occurrences union
            {( oid := o.oid, sid := o.donorId, cands := 
              ( for t in o.transcript_consequences union
                  for c in cnvCases1 union
                    if (t.gene_id == c.cn_gene_id && c.cn_case_uuid == o.donorId) 
                    then {( gene := t.gene_id, score := (c.cn_copy_number + 0.01) * if (t.impact == "HIGH") then 0.80 
                          else if (t.impact == "MODERATE") then 0.50
                          else if (t.impact == "LOW") then 0.30
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
                if (s.bcr_patient_uuid == o.donorId) then
                  {( mutId := o.oid, scores := 
                    ( for t in o.transcript_consequences union
                        for c in copynumber union
                          if (t.gene_id == c.cn_gene_id && c.cn_aliquot_uuid == s.bcr_aliquot_uuid) then
                            {( gene := t.gene_id, score := (c.cn_copy_number + 0.01) * if (t.impact == "HIGH") then 0.80 
                                else if (t.impact == "MODERATE") then 0.50
                                else if (t.impact == "LOW") then 0.30
                                else 0.01 )}).sumBy({gene}, {score}) )} )}
      """

    // val query1 = parser.parse(querySimple).get.asInstanceOf[JsonWriter.Program]

    // val s = JsonWriter.produceJsonString(query1).replace("\n", "")
    // val jsValue = Json parse s
    // val pj = Json prettyPrint jsValue

    val soSimple = 
      s"""
        ShredTest <= for o in occurrences union {(sid := o.donorId, cons := for t in o.transcript_consequences union {(gene := t.gene_id)})}
      """

    val simple = 
      s"""
        Simple <= 
        for s in samples union
         {(  id := s.bcr_patient_uuid, mutations :=
            for o in occurrences union
                if (s.bcr_patient_uuid == o.oid) then
                {(  mutid := o.oid)})}
      """

    val bugfix = 
      s"""
        BUG <= 
          for s in samples union
            {(  sid := s.bcr_patient_uuid, mutations :=
              (  for o in occurrences union
                  for cc in o.transcript_consequences union
                    {(  gene := cc.gene_id, score := cc.polyphen_score)}).sumBy({gene}, {score}))}
      """

    val query1 = 
      s"""
        Test <= 
        for s in samples union
          {(  sid := s.bcr_patient_uuid, mutations :=
            (  for o in occurrences union
                if (s.bcr_patient_uuid == o.donorId) then
                for cc in o.transcript_consequences union
                   {(  gene := cc.gene_id, score := cc.polyphen_score)}).sumBy({gene}, {score}))}
      """

    val s = parseProgram(query1, shred = false).replace("\n", "")
    println(s)
    val jsValue = Json parse s
    val pj = Json prettyPrint jsValue

  // val printer = new PrintWriter(new FileOutputStream(new File("test.json"), false))
 //    printer.println(pj)
 //    printer.close

}