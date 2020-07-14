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
class SparkDatasetGenerator(cache: Boolean, evaluate: Boolean, skew: Boolean = false, optLevel: Int = 2,
  unshred: Boolean = false, evalFinal: Boolean = true, inputs: Map[Type, String] = Map()) extends SparkTypeHandler with SparkUtils {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs
  var encoders: Map[String, Type] = Map()
  override val bagtype: String = "Seq"
  val ext = new Extensions{}

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
    case r => 
      s"""|implicit val encoder${r._1}: Encoder[${r._1}] = Encoders.product[${r._1}]
          |${generateUdf(r._2)}
          |""".stripMargin
  }.mkString("\n")

  def generateUdf(tp: Type): String = {
    val args = tp.attrs.map(t => s"${t._1}: ${generateType(t._2)}").mkString("(", ",", ")")
    val inpts = tp.attrs.map(t => t._1).mkString(",")
    s"val udf${generateType(tp)} = udf { $args => ${generateType(tp)}($inpts) }"
  }

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

  private def matchOption(v: Variable, attr: String): String = {
    v.tp.attrs(attr) match {
      case OptionType(otp) => 
        s"${v.name} => ${v.name}.$attr match { case Some($attr) => $attr; case _ => ${zero(otp)} }"
      case _ => s"${v.name} => ${v.name}.$attr"
    }
  }

  private def matchOption(v: Variable, attr: String, rattr: Double): String = {
    v.tp.attrs(attr) match {
      case OptionType(otp) => 
        s"${v.name} => ${v.name}.$attr match { case Some($attr) => $rattr; case _ => 0.0 }"
      case _ => s"${v.name} => ${v.name}.$attr"
    }
  }

  /** This is called from functions, not Dataset operations.
    * When a value is an option, then it will call get on it 
    */
  private def accessOption(e: CExpr, nv: Variable): String = e match {
    case Project(v, field) => 
      nv.tp.attrs.getOrElse(field, v.tp.attrs(field)) match {
        case _:OptionType => s"${nv.name}.$field.get"
        case _ => s"${nv.name}.$field"
      }
    case Record(fs) => 
      val unouter = e.tp.unouter
      handleType(unouter)
      val rcnts = fs.map(f => accessOption(f._2, nv)).mkString(", ")
      s"${generateType(unouter)}($rcnts)"
    case If(cond, e1, Some(e2)) => 
      s"if (${accessOption(cond, nv)}) ${accessOption(e1, nv)} else ${accessOption(e2, nv)} "
    case Equals(e1, e2) => s"${accessOption(e1, nv)} == ${accessOption(e2, nv)}"
    case Lt(e1, e2) => s"${accessOption(e1, nv)} < ${accessOption(e2, nv)}"
    case Gt(e1, e2) => s"${accessOption(e1, nv)} > ${accessOption(e2, nv)}"
    case Lte(e1, e2) => s"${accessOption(e1, nv)} <= ${accessOption(e2, nv)}"
    case Gte(e1, e2) => s"${accessOption(e1, nv)} >= ${accessOption(e2, nv)}"
    case And(e1, e2) => s"${accessOption(e1, nv)} && ${accessOption(e2, nv)}"
    case Or(e1, e2) => s"${accessOption(e1, nv)} || ${accessOption(e2, nv)}"
    case Not(e1) => s"!(${accessOption(e1, nv)})"
    case MathOp(op, e1, e2) => 
      s"${accessOption(e1, nv)} $op ${accessOption(e2, nv)}"     
    case _ => generate(e)
  }

  /** This is specific to Dataset, when there is a projection 
    * then it is treated as a column reference
    */
  def generateReference(e: CExpr, literal: Boolean = false): String = e match {
    case Project(v, f) => v.tp match {
      case LabelType(_) => s"""col("_LABEL").getField("$f")"""
      case _ => s"""col("$f")"""
    }
    case Label(fs) if fs.size == 1 => generateReference(fs.head._2)
    case Label(fs) => 
      encoders = encoders + (generateType(e.tp) -> e.tp)
      handleType(e.tp)
      val rcnts = e.tp.attrs.map(f => generateReference(fs(f._1))).mkString(", ")
      s"udf${generateType(e.tp)}($rcnts)"
    case Constant(c) if literal => s"lit(${generate(e)})"
    case If(cond, e1, Some(e2)) => 
      s"when(${generate(cond)}, ${generateReference(e1)}).otherwise(${generateReference(e2)})"
    case If(cond, e1, None) => 
      s"when(${generate(cond)}, ${generateReference(e1)})"
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
    case CUdf(n, e1, tp) => s"$n(${generateReference(e1)})"
	  case Sng(e) => s"Seq(${generate(e)})"
    case CGet(e1) => s"${generate(e1)}.head"
    case CDeDup(e1) => s"${generate(e1)}.distinct"
    case Label(fs) if fs.size == 1 => generate(fs.head._2)
    case Record(fs) if fs.contains("element") && fs.size == 1 => fs("element")
    case Record(fs) => {
      handleType(e.tp)
      val rcnts = e.tp.attrs.map(f => generate(fs(f._1))).mkString(", ")
      s"${generateType(e.tp)}($rcnts)"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"

    case Project(e1, "_LABEL") => s"${generate(e1)}"
    case Project(e1, "element") => s"${generate(e1)}"
    case Project(e2 @ Record(fs), field) => 
      s"${generate(e2)}.${kvName(field)(fs.size)}"
    case Project(e2, field) => s"${generate(e2)}.$field"
    /** MATH OPS **/
    case MathOp(op, e1, e2) => op match {
      case OpDivide => 
        val ge2 = generateReference(e2)
        val ze2 = zero(e2.tp)
        s"when($ge2 === $ze2, $ze2).otherwise(${generateReference(e1)} $op $ge2)"
      case _ => s"(${generateReference(e1)} $op ${generateReference(e2)})"
    }

    /** BOOL OPS **/
    case Equals(e1, e2) => 
      val eq = (e1, e2) match {
        case (Project(p1, _), _) if p1.tp.isInstanceOf[LabelType] => "=="
        case (_, Project(p2, _)) if p2.tp.isInstanceOf[LabelType] => "=="
        case _ => "==="
      }
      s"${generateReference(e1)} $eq ${generateReference(e2)}"
    case Lt(e1, e2) => s"${generateReference(e1)} < ${generateReference(e2)}"
    case Gt(e1, e2) => s"${generateReference(e1)} > ${generateReference(e2)}"
    case Lte(e1, e2) => s"${generateReference(e1)} <= ${generateReference(e2)}"
    case Gte(e1, e2) => s"${generateReference(e1)} >= ${generateReference(e2)}"
    case And(e1, e2) => s"${generateReference(e1)} && ${generateReference(e2)}"
    case Or(e1, e2) => s"${generateReference(e1)} || ${generateReference(e2)}"
    case Not(Equals(e1, e2)) => s"${generateReference(e1)} =!= ${generateReference(e2)}"
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

      val nfields = ext.collect(pat)
      println(v.tp.attrs.keySet)
      println(in.tp.attrs.keySet)
      println(nfields)
      val select = if (in.tp.attrs.keySet == nfields) ""
        else s".select(${nfields.toList.mkString("\"", "\", \"", "\"")})"

        // if (fields.isEmpty) ""
        // else if (fields.toSet == ep.inputColumns) ""
        // else s".select(${(fields.toSet & ep.inputColumns).mkString("\"", "\", \"", "\"")})"

      // input table attributes
      val projectCols = fields.toSet
      // output attributes
      val newCols = pat.fields.keySet

      // make new columns
      val newColumns = pat.fields.flatMap{
        // avoid column renaming
        case (col, Project(_, oldCol)) if col != oldCol => Nil
        case (col, expr) if !projectCols(col) =>
          List(s"""|  .withColumn("$col", ${generateReference(expr, true)})""")
        case (ncol, col @ Label(fs)) => 
          List(s"""|.withColumn("$ncol", ${generateReference(col)})""")
 		    case _ => Nil
      }

      // override existing columns
      val renamedColumns = pat.fields.flatMap{
        // create a new column from an old column
        case (col, Project(_, oldCol)) if col != oldCol => 
          // creates an additional column
          if (newCols(oldCol)) List(s"""| .withColumn("$col", $$"$oldCol")""")
          // overrides a column
          else List(s"""| .withColumnRenamed("$oldCol", "$col")""")
		      case _ => Nil
      } 

      // ensure that new columns are made before renaming occurs
      val columns = (newColumns ++ renamedColumns).mkString("\n").stripMargin

      s"""|${generate(in)}$select
          $columns
          | .as[$nrec]
          |""".stripMargin

    case ej @ DFOuterJoin(left, v1, right, v2, Equals(Project(_, p1), Project(_, p2 @ "_1")), filt) if right.tp.isDict =>
   
    // adjust lookup column of dictionary
      val rcol = s"${p1}${p2}"
      val rtp = rename(right.tp.attrs, p2, rcol)
	    handleType(rtp)
      val grtp = generateType(rtp)

      // adjust label lookup column
      val lcol = s"${p1}_LABEL"
      val (lcolumn, ltp) = v1.tp.attrs get "_LABEL" match {
        case Some(LabelType(ms)) if ms.size > 1 =>
          (s""".withColumn("$lcol", col("_LABEL").getField("$p1"))""", 
            RecordCType(left.tp.attrs))
        case _ => 
          (s""".withColumnRenamed("${p1}", "$lcol")""",
            rename(left.tp.attrs, p1, lcol))
      }
      handleType(ltp)
      val gltp = generateType(ltp)

      val nrecTp = RecordCType(ltp.merge(rtp).attrs -- Set(rcol,lcol))
      handleType(nrecTp)
      val classTags = if (!skew) ""
        else s"[$grtp, ${generateType(right.tp.attrs(p2))}]"

      s"""|${generate(left)}$lcolumn
          |   .as[$gltp].equiJoin$classTags(
          |   ${generate(right)}.withColumnRenamed("${p2}", "$rcol").as[$grtp], 
          |   Seq("$lcol", "$rcol"), "left_outer").drop("$lcol", "$rcol")
          |   .as[${generateType(nrecTp)}]
          |""".stripMargin

    // JOIN operator
    case ej:JoinOp => 

      handleType(ej.tp.tp)
      val nrec = generateType(ej.tp.tp)
      val gright = generate(ej.right)
      val rtp = ej.right.tp.attrs

      if (ej.isEquiJoin){

          val (p1, p2) = (ej.p1, ej.p2)

          val classTags = if (!skew) ""
            else {
              handleType(RecordCType(rtp))
              s"[${generateType(RecordCType(rtp))}, ${generateType(rtp(p2))}]"
            }

          s"""|${generate(ej.left)}.equiJoin$classTags($gright, 
              | Seq("${p1}", "${p2}"), "${ej.jtype}").as[$nrec]
              |""".stripMargin

        }else {
          ej.cond match {
            case Constant(true) => 
              s"""|${generate(ej.left)}.crossJoin($gright)
                  |  .as[$nrec]
                  |""".stripMargin 
            case _ => 
              s"""|${generate(ej.left)}.join($gright, ${generateReference(ej.cond)}, "${ej.jtype}")
                  |  .as[$nrec]
                  |""".stripMargin   
          }      
        }
    
    case eu @ DFUnnest(in, v, path, v2, filter, fields) =>

      val topAttrs = v.tp.project(fields).attrs.map(f => f._1 -> Project(v, f._1)) - path
      val nextAttrs = v2.tp.project(fields).attrs.map(f => f._1 -> Project(v2, f._1))
      val nrec = Record(topAttrs ++ nextAttrs)
      val gv = generate(v)

      s"""|${generate(in)}.flatMap{ case $gv => 
          | $gv.$path.map( ${generate(v2)} => ${generate(nrec)} )
          |}.as[${generateType(nrec.tp)}]
          |""".stripMargin

    case eu @ DFOuterUnnest(in, v, path, v2, filter, fields) =>

      val topAttrs = v.tp.project(fields).attrs.map(f => f._1 -> Project(v, f._1)) - path
      val nv2 = Variable(v2.name, v2.tp.unouter)
      val nextAttrs = nv2.tp.project(fields).attrs.map(f => f._1 -> COption(Project(nv2, f._1)))
      val nrec0 = topAttrs ++ nv2.tp.project(fields).attrs.map(f => f._1 -> COption(Null))

      val nrec1 = Record(topAttrs ++ nextAttrs)
      val gnrec1 = generate(nrec1)
      val gnrec0 = s"${generateType(nrec1.tp)}(${nrec0.map(f => generate(f._2)).mkString(", ")})"
      val gv = generate(v)
      val getPath = v.tp.attrs(path) match {
        case OptionType(_) => 
          s"""|   $gv.$path match {
              |     case Some($path) if $path.nonEmpty => $path.map( ${generate(nv2)} => $gnrec1 )
              |     case _ => Seq($gnrec0)
              |   }
              |""".stripMargin
        case _ => 
          s"""|   if ($gv.$path.isEmpty) Seq($gnrec0)
              |   else $gv.$path.map( ${generate(nv2)} => $gnrec1 )
              |""".stripMargin
      }
      s"""|${generate(in)}.flatMap{
          | case $gv => 
          |   $getPath
          |}.as[${generateType(nrec1.tp)}]
          |""".stripMargin

    // Nest - Join => CoGroup
    // if value contains only attributes from right relation
    // note this also handles lookup in unshredding
    case Bind(vj, join:JoinOp, Bind(nv, nd @ DFNest(in, v, key, value @ Record(fs), filter, nulls, tag), e2)) 
      if optLevel == 20 && (ext.collect(value) subsetOf join.v2.tp.attrs.keySet) && join.isEquiJoin =>
      
      val (p1, p2) = join.cond match {
        case Equals(Project(_, c1), Project(_, c2)) => join.v.tp.attrs get c1 match {
          case Some(_) => (c1, c2)
          case _ => (c2, c1)
        }
        case _ => sys.error("condition not supported")
      }
      val gv = generate(join.v)
      val gv2 = generate(join.v2)
      val gv3 = generate(v)
      val gright = s"${generate(join.right)}.unionGroupByKey($gv => $gv.${p2})"

      handleType(nd.tp.tp)
      val nrec = generateType(nd.tp.tp)
      val frec = getRecord(nrec, gv3, nd.tp.attrs, tag, "grp")

      val gvalue = accessOption(value, join.v2)
      val leftMap = join.jtype match {
        case "left_outer" => s"ve1.map($gv3 => $frec)"
        case "inner" => "ve1.flatMap($gv3 => if (grp.nonEmpty) $frec else Seq())"
        case _ => sys.error("unsupported join type")
      }

      s"""|val ${generate(nv)} = ${generate(join.left)}.cogroup($gright, $gv => $gv.${p1})(
          |   (_, ve1, ve2) => {
          |     val grp = ve2.map($gv2 => $gvalue).toSeq
          |     $leftMap
          |   }).as[$nrec]
          |${generate(e2)}
          |""".stripMargin

    // primitive monoid
    case en @ DFNest(in, v, key, Constant(c), Record(fs), nulls, tag) =>
      handleType(en.tp.tp)
      val intp = generateType(in.tp.asInstanceOf[BagCType].tp)
      val gtp = generateType(en.tp.tp)

      val gv = generate(v)
      val rkey = Record(key.map(k => k -> Project(v, k)).toMap)
      val rvalues = fs.keySet.map(vs => 
        s"typed.sum[$intp](${matchOption(v, vs, c.asInstanceOf[Double])})\n").toList.mkString("agg(", ",", ")")

      val v2 = generate(Variable.fresh(StringType))
      val nrec = en.tp.tp.attrs.map(k => 
        if (k._1 == tag) s"$v2._2" else s"$v2._1.${k._1}").mkString(s"$gtp(", ", ", ")")

      s"""|${generate(in)}.groupByKey($gv => ${generate(rkey)})
          | .$rvalues.mapPartitions{ it => it.map{ $v2 =>
          |   $nrec
          |}}.as[$gtp]
          |""".stripMargin

    case en @ DFNest(in, v, key, value, filter, nulls, tag) =>
      
      val gv = generate(v)
      val rkey = Record(key.map(k => k -> Project(v, k)).toMap)
      val kv = Variable("key", rkey.tp)
      val grpv = Variable("grp", BagCType(value.tp.unouter))
      val frec = Record(en.tp.tp.attrs.map(k => if (k._1 == tag) k._1 -> grpv 
        else k._1 -> Project(kv, k._1)).toMap)
     
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
        if (value.contains(k._1)) k._2 match {
          case _:OptionType => s"Some(${k._1})"
          case _ => k._1 
        }else s"key.${k._1}").mkString(s"$gtp(", ", ", ")")

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

    // filter
    case Select(x, v, filt, e2) => 
      val filter = filt match {
        case Constant(true) => ""
        case _ => s".filter(${generateReference(filt)})"
      }
      if ((v.tp.attrs.keySet -- e2.tp.attrs.keySet).isEmpty) 
        s"${generate(x)}$filter"
      else {
        handleType(e2.tp)
        val cols = e2.tp.attrs.keySet.toList.map(c => "\""+c+"\"").mkString(",")
        s"""|${generate(x)}.select($cols)$filter
            | .as[${generateType(e2.tp)}]""".stripMargin
      }

    case Bind(v, CNamed(n, e1), LinearCSet(fs)) =>
      val gtp = if (skew) "[Int]" else ""
      val repart = if (n.contains("MDict")) s""".repartition$gtp($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.print
          |${if (!cache) comment(n) else n}.cache
          |${if (!cache && !evalFinal) comment(n) else n}.count
          |""".stripMargin

    case Bind(v, CNamed(n, e1), e2) =>
      val gtp = if (skew) "[Int]" else ""
      val repart = if (n.contains("MDict")) s""".repartition$gtp($$"_1")""" else ""
      val gv = generate(v)
      s"""|val $gv = ${generate(e1)}
          |val $n = $gv$repart
          |//$n.print
          |${if (!cache || evalFinal) comment(n) else n}.cache
          |${if (!evaluate) comment(n) else n}.count
          |${generate(e2)}
          |""".stripMargin

    case LinearCSet(fs) => ""
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"
    case _ => s"/** TODO: $e **/"
  }

}
