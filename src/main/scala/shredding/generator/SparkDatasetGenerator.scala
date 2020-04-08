package shredding.generator

import shredding.core._
import shredding.wmcc.{Multiply => CMultiply}
import shredding.wmcc._
import shredding.utils.Utils.ind

class SparkDatasetGenerator(cache: Boolean, evaluate: Boolean, skew: Boolean = false,
  unshred: Boolean = false, inputs: Map[Type, String] = Map()) extends SparkTypeHandler with SparkUtils {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs
  override val bagtype: String = "Seq"

  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }

  def generateEncoders(): String = "TODO"

  def drop(tp: Type, v: Variable, field: String, index: Boolean = true): CExpr = tp match {
    case RecordCType(fs) => 
      val imap = if (index) Map("index" -> Index) else Map()
      Record(imap ++ (fs - field).map{ case (
        attr, atp) => attr -> Project(v, attr)})
    case _ => sys.error(s"unsupported type ${tp}")
  }

  def generate(e: CExpr): String = e match {

    /** ZEROS **/
    case Null => "null"
    case CUnit => "()"
    case EmptySng => "Seq()"
    case EmptyCDict => s"()"
    case Index => "index"
    
    /** BASIC CONSTRUCTS **/
    case Variable(name, _) => name
    case InputRef(name, tp) => name
    case Constant(s:String) => "\"" + s + "\""
    case Constant(x) => x.toString
    case Sng(e) => s"Seq(${generate(e)})"
    case Label(fs) => {
      val tp = e.tp
      handleType(tp)
      val inner = fs.map{f => generate(f._2)}.mkString(", ")
      s"${generateType(tp)}($inner)"
    }
    case Record(fs) => {
      val tp = e.tp
      handleType(tp)
      s"${generateType(tp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"
    // this is a quick hack
    case Project(e1, "_LABEL") => generate(e1)
    case Project(e2 @ Record(fs), field) => 
      s"${generate(e2)}.${kvName(field)(fs.size)}"
    case Project(e2, field) => s"${generate(e2)}.$field"
    /** MATH OPS **/
    case CMultiply(e1, e2) => s"${generate(e1)} * ${generate(e2)}"

    /** BOOL OPS **/
    case Equals(e1, e2) => s"${generate(e1)} == ${generate(e2)}"
    case Lt(e1, e2) => s"${generate(e1)} < ${generate(e2)}"
    case Gt(e1, e2) => s"${generate(e1)} > ${generate(e2)}"
    case Lte(e1, e2) => s"${generate(e1)} <= ${generate(e2)}"
    case Gte(e1, e2) => s"${generate(e1)} >= ${generate(e2)}"
    case And(e1, e2) => s"${generate(e1)} && ${generate(e2)}"
    case Or(e1, e2) => s"${generate(e1)} || ${generate(e2)}"
    case Not(e1) => s"!(${generate(e1)})"

    case FlatDict(e1) => s"${generate(e1)} /** FLATTEN **/"
    case GroupDict(e1) => generate(e1) 

    case Join(e1, e2, v1, Project(_, p1), v2, Project(_, p2), proj1, proj2) =>
      val recTp = flatRecord(e1, e2)
      handleType(recTp)
      val rec = generateType(recTp)
      val ge1 = generate(e1)
      val ge2 = generate(e2)
      s"""|$ge1.join($ge2, 
          | $ge1("$p1") === $ge2("$p2")).as[$rec]
          |""".stripMargin

    case Bind(_, Lookup(e1, e2, _, p1 @ Project(v1, f1), v2, p2, v3),
      Bind(rv, Reduce(re1, v, f:Record, Constant(true)), e3)) =>
      val glv = generate(rv)
      val ge1 = generate(e1)
      val ge2 = generate(e2)
      val gv1 = generate(v1)
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val ve3 = "x" + Variable.newId()
      val ve4 = "x" + Variable.newId()
      val nrec = drop(v2.tp, v2, "_1", false)
      val gnrec = generate(nrec)

      val f2tp = RecordCType(flattenLabelType(f.tp.attrTps, f1))
      handleType(f2tp)
      val gnrecName2 = generateType(f2tp)
      val gnrec2 = f.fields.map{ case (attr, value) => value match {
        case Project(pv1, f1) => s"$ve4.$f1"
        case v:Variable => s"$ve3"
        case _ => ???
      }}.mkString(s"$gnrecName2(", ",", ")")
      s"""|implicit val encoder${Variable.newId()} = Encoders.product[${generateType(nrec.tp)}]
          |val $glv = ${generate(e1)}.groupByKey(${generate(v1)} => ${generate(p1)})
          | .cogroup($ge2.groupByKey(x => x._1))(
          |   (_, $ve1, $ve2) => {
          |     val $ve3 = $ve2.map(${generate(v2)} => $gnrec).toSeq
          |     $ve1.map($ve4 => $gnrec2)
          |   }
          | ).as[$gnrecName2]
          |${generate(e3)}
          |""".stripMargin

    /** IDENTITY **/
    case Reduce(InputRef(n, _), v, Variable(_,_), Constant(true)) => n
    case Reduce(FlatDict(i), v, Variable(_,_), Constant(true)) => generate(i)
    case Reduce(Variable(n, _), v, Variable(_,_), Constant(true)) => n

    /** PROJECT **/
    case Reduce(e1, v, f @ Record(_), Constant(true)) =>
      val frec = flattenLabel(f)
      handleType(frec.tp)
      val rec = generateType(frec.tp)
      val projsMap = project(frec)
      val projs = projsMap.values.mkString("\"", "\", \"", "\"")
      val tblRows = flatType(v).attrTps.keySet
      val newRows = projsMap.values.toSet
      if ((tblRows intersect newRows) == tblRows){
        val labelCols = newRows -- tblRows
        s"""|${generate(e1)}
            ${renameColumns(projsMap, tblRows)}.as[$rec]
            |""".stripMargin
      }
      else 
        s"""|${generate(e1)}.select($projs)
            ${renameColumns(projsMap)}.as[$rec]
            |""".stripMargin

    // catch all
    case Select(x, v, p, e2) => generate(Reduce(x, List(v), e2, p))

    case Bind(v, CNamed(n, e1), LinearCSet(fs)) =>
      val repart = if (n.contains("MDict")) s""".repartition($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.collect.foreach(println(_))
          |${if (!cache) comment(n) else n}.cache
          |${if (!unshred && !evaluate) comment(n) else n}.count
          |""".stripMargin

    case Bind(v, CNamed(n, e1), e2) =>
      val repart = if (n.contains("MDict")) s""".repsartition($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.collect.foreach(println(_))
          |${if (!cache) comment(n) else n}.cache
          |${if (!evaluate) comment(n) else n}.count
          |${generate(e2)}
          |""".stripMargin

    case LinearCSet(fs) => ""
      // fs.map(generate(_)+"/** this **/").mkString("\n")
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"
    case _ => s"/** TODO: $e **/"
  }

}