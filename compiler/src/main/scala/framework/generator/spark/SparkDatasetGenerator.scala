package framework.generator.spark

import framework.common._
// import framework.plans.{Multiply => CMultiply}
import framework.plans._
import framework.utils.Utils.ind

/**
  * Spark/Scala generator for Datasets. 
  * This is the initial prototype implementation and is actively in development.
  * @deprecated no longer supported, see dataset generator
  * @param cache boolean flag for caching intermediate outputs
  * @param evaluate boolean flag for evaluating intermediate outputs
  * @param skew boolean flag for skew-aware application (requires only minor adjustments)
  * @param isDict boolean flag to represent coming from shredded pipeline
  * @param unshred boolean flag to represent unframework 
  * @param evalFinal boolean flag to run evaluate on the final output (standard pipeline)
  * @param inputs map of input types that should not be reproduced (important for programs)
  */
class SparkDatasetGenerator(cache: Boolean, evaluate: Boolean, skew: Boolean = false, isDict: Boolean = true,
  unshred: Boolean = false, evalFinal: Boolean = true, inputs: Map[Type, String] = Map()) extends SparkTypeHandler with SparkUtils {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs
  var encoders: Set[String] = Set()
  override val bagtype: String = "Seq"

  /** Generates the code for the set of case class records associated to the 
    * records in the generated program.
    *
    * @param names list of names to omit from header creation
    * @return string representing all the records required to run the corresponding application
    */
  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }

  /** Generates the code for the set of implicit encoders associated to the 
    * records in the generated program.
    *
    * @return string representing all encoders required by the application
    */
  def generateEncoders(): String = encoders.map{
    case r => s"implicit val encoder$r: Encoder[$r] = Encoders.product[$r]"
  }.mkString("\n")

  /**
    * Drop a bag attribute and create an index where necessary.
    * 
    * @param tp type of record to drop attribute from
    * @param v variable for substitution within record attribute expressions
    * @param field bag projection attribute
    * @param index boolean flag to introduce index or not
    * @return new record with projected bag type
    */
  private def drop(tp: Type, v: Variable, field: String, index: Boolean = true): CExpr = tp match {
    case RecordCType(fs) => 
      val imap = if (index) Map("index" -> Index) else Map()
      Record(imap ++ (fs - field).map{ case (
        attr, atp) => attr -> Project(v, attr)})
    case _ => sys.error(s"unsupported type ${tp}")
  }

  /** Generates a single variable or a tuple of variables
    * never more than two, ie. either a or (a,b)
    *
    * @param list of variables from left subplan
    * @return string tupled vars
    */
  private def vars(vs: List[Variable]): String = {
    if (vs.size == 1) generate(vs.head)
    else vs.map(generate(_)).mkString("(", ",", ")")
  }

  private def matchOption(v: Variable, attr: String): String = 
    v.tp.attrs(attr) match {
      case OptionType(otp) => s"${v.name} => ${v.name}.$attr match { case Some($attr) => $attr; case _ => ${zero(otp)} }"
      case _ => s"${v.name} => ${v.name}.$attr"
    }

  // v is an attribute that adjusts the variable naming in the 
  // generator
  private def accessOption(e: CExpr, nv: Variable): String = e match {
    case Project(v, field) => 
      nv.tp.attrs(field) match {
        case _:OptionType => s"${nv.name}.$field.get"
        case _ => s"${nv.name}.$field"
      }
    case Record(fs) => 
      val unouter = e.tp.unouter
      handleType(unouter)
      val rcnts = fs.map(f => accessOption(f._2, nv)).mkString(", ")
      s"${generateType(unouter)}($rcnts)"
  }

  def generateReference(e: CExpr): String = e match {
    case Project(_, f) => "col(\""+f+"\")"
    case Equals(e1, e2) => s"${generateReference(e1)} === ${generateReference(e2)}"
    case _ => generate(e)
  }

  def generate(e: CExpr): String = e match {
    /** ZEROS **/
    case Null => "null"
    case CUnit => "()"
    case EmptySng => "Seq()"
    case EmptyCDict => s"()"
    case Index => "index"
    case COption(e1) => e1 match {
      case Null => "None"
      case _ => s"Some(${generate(e1)})"
    }
    
    /** BASIC CONSTRUCTS **/
    case Variable(name, _) => name
    case InputRef(name, tp) => name
    case Constant(s:String) => "\"" + s + "\""
    case Constant(x) => x.toString
    case Sng(e) => s"Seq(${generate(e)})"
    case Label(fs) if fs.size == 1 => generate(fs.head._2)
    case Record(fs) => {
      val tp:RecordCType = e.tp.asInstanceOf[RecordCType]
      handleType(tp)
      val rcnts = tp.attrTps.map(f => generate(fs(f._1))).mkString(", ")
      s"${generateType(tp)}($rcnts)"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"

    case Project(e1, "_LABEL") => generate(e1)
    case Project(e2 @ Record(fs), field) => 
      s"${generate(e2)}.${kvName(field)(fs.size)}"
    case Project(e2, field) => s"${generate(e2)}.$field"
    /** MATH OPS **/
    case Multiply(e1, e2) => s"${generateReference(e1)} * ${generateReference(e2)}"
    case Divide(e1, e2) => 
      val ge2 = generateReference(e2)
      val ze2 = zero(e2.tp)
      s"when($ge2 === $ze2, $ze2).otherwise(${generateReference(e1)} / $ge2)"

    /** BOOL OPS **/
    case Equals(e1, e2) => s"${generateReference(e1)} === ${generateReference(e2)}"
    case Lt(e1, e2) => s"${generateReference(e1)} < ${generateReference(e2)}"
    case Gt(e1, e2) => s"${generateReference(e1)} > ${generateReference(e2)}"
    case Lte(e1, e2) => s"${generateReference(e1)} <= ${generateReference(e2)}"
    case Gte(e1, e2) => s"${generateReference(e1)} >= ${generateReference(e2)}"
    case And(e1, e2) => s"${generateReference(e1)} && ${generateReference(e2)}"
    case Or(e1, e2) => s"${generateReference(e1)} || ${generateReference(e2)}"
    case Not(e1) => s"!(${generateReference(e1)})"

    case If(cond, s1, Some(s2)) => 
      s"when(${generate(cond)}, ${generate(s1)}).otherwise(${generate(s2)})"
    case If(cond, s1, None) => 
      s"when(${generate(cond)}, ${generate(s1)})"

    case FlatDict(e1) => s"${generate(e1)}"
    case GroupDict(e1) => generate(e1) 

    case DFProject(in, v, Constant(true), Nil) => generate(in)

    case ep @ DFProject(in, v, pat:Record, fields) =>
      handleType(pat.tp)
      val nrec = generateType(pat.tp)
      val select = if (fields.isEmpty) ""
        else s".select(${fields.mkString("\"", "\", \"", "\"")})"
      
      val rename = ep.rename.map{
        case (newCol, oldCol) => s"""|.withColumnRenamed("$oldCol", "$newCol")"""
      }.mkString("\n").stripMargin
      val append = ep.makeCols.map{
        case (name, expr) => s"""|.withColumn("$name", ${generate(expr)})"""
      }.mkString("\n").stripMargin

      s"""|${generate(in)}$select$append
          $rename.as[$nrec]
          |""".stripMargin

    case ej @ DFJoin(left, v, p1, right, v2, p2, Nil) => 
      handleType(ej.tp.tp)
      val nrec = generateType(ej.tp.tp)
      s"""|${generate(left)}.join(${generate(right)}, 
          | col("$p1") === col("$p2")).as[$nrec]
          |""".stripMargin

    case ej @ DFOuterJoin(left, v, p1, right, v2, p2, Nil) => 
      handleType(ej.tp.tp)
      val nrec = generateType(ej.tp.tp)
      s"""|${generate(left)}.join(${generate(right)}, 
          | col("$p1") === col("$p2"), "left_outer").as[$nrec]
          |""".stripMargin

    case eu @ DFUnnest(in, v, path, v2, filter, fields) =>

      val topAttrs = v.tp.attrs.map(f => f._1 -> Project(v, f._1)) - path
      val nextAttrs = v2.tp.attrs.map(f => f._1 -> Project(v2, f._1))
      val nrec = Record(topAttrs ++ nextAttrs)
      val gv = generate(v)

      s"""|${generate(in)}.flatMap{ case $gv => 
          | $gv.$path.map( ${generate(v2)} => ${generate(nrec)} )
          |}.as[${generateType(nrec.tp)}]
          |""".stripMargin

    case eu @ DFOuterUnnest(in, v, path, v2, filter, fields) =>

      val topAttrs = v.tp.attrs.map(f => f._1 -> Project(v, f._1)) - path
      val nv2 = Variable(v2.name, v2.tp.unouter)
      val nextAttrs = nv2.tp.attrs.map(f => f._1 -> COption(Project(nv2, f._1)))
      val nrec0 = topAttrs ++ nv2.tp.attrs.map(f => f._1 -> COption(Null))

      val nrec1 = Record(topAttrs ++ nextAttrs)
      val gnrec1 = generate(nrec1)
      val gnrec0 = s"${generateType(nrec1.tp)}(${nrec0.map(f => generate(f._2)).mkString(", ")})"
      val gv = generate(v)
      s"""|${generate(in)}.flatMap{
          | case $gv => if ($gv.$path.isEmpty) Seq($gnrec0)
          |   else $gv.$path.map( ${generate(nv2)} => $gnrec1 )
          |}.as[${generateType(nrec1.tp)}]
          |""".stripMargin

    case en @ DFNest(in, v, key, value, filter, nulls) =>
      val gv = generate(v)
      val rkey = Record(key.map(k => k -> Project(v, k)).toMap)
      val kv = Variable("key", rkey.tp)
      val grpv = Variable("grp", BagCType(value.tp.unouter))
      val frec = Record(en.tp.tp.attrs.map(k => if (k._1 == "_2") k._1 -> grpv else k._1 -> Project(kv, k._1)).toMap)
      s"""|${generate(in)}.groupByKey($gv => ${generate(rkey)}).mapGroups{
          | case (key, value) => 
          |   val grp = value.flatMap($gv => 
          |    $gv.${nulls.head} match {
          |      case None => Seq()
          |      case _ => Seq(${accessOption(value, v)})
          |   }).toSeq
          |   ${generate(frec)}
          | }.as[${generateType(frec.tp)}]
          |""".stripMargin

    case er @ DFReduceBy(in, v, key, value) => 

      handleType(er.tp.tp)
      val intp = generateType(in.tp.asInstanceOf[BagCType].tp)
      val gtp = generateType(er.tp.tp)

      val gv = generate(v)
      val rkey = Record(key.map(k => k -> Project(v, k)).toMap)
      val rvalues = value.map(vs => 
        s"typed.sum[$intp](${matchOption(v, vs)})\n").mkString("agg(", ",", ")")

      val nrec = er.tp.tp.attrs.map(k => 
        if (value.contains(k._1)) k._1 
        else s"key.${k._1}").mkString(s"$gtp(", ", ", ")")

      s"""|${generate(in)}.groupByKey($gv => ${generate(rkey)})
          | .$rvalues.mapPartitions{ it => it.map{ case (key, ${value.mkString(", ")}) =>
          |   $nrec
          |}}.as[$gtp]
          |""".stripMargin

    case ei @ AddIndex(e1, name) => 
      handleType(ei.tp.tp)
      val nrec = generateType(ei.tp.tp)
      s"""|${generate(e1)}.withColumn("$name", monotonically_increasing_id())
          | .as[$nrec]
          |""".stripMargin

    // catch all
    case Select(x, v, p, e2) => generate(x)

    case Bind(v, CNamed(n, e1), LinearCSet(fs)) =>
      val gtp = if (skew) "[Int]" else ""
      val repart = if (n.contains("MDict")) s""".repartition$gtp($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.print
          |${if (!cache) comment(n) else n}.cache
          |${if (!evalFinal) comment(n) else n}.count
          |""".stripMargin

    case Bind(v, CNamed(n, e1), e2) =>
      val gtp = if (skew) "[Int]" else ""
      val repart = if (n.contains("MDict")) s""".repartition$gtp($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.print
          |${if (!cache) comment(n) else n}.cache
          |${if (!evaluate) comment(n) else n}.count
          |${generate(e2)}
          |""".stripMargin

    case LinearCSet(fs) => ""
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"
    case _ => s"/** TODO: $e **/"
  }

}
