package shredding.generator

import shredding.core._
import shredding.wmcc.{Multiply => CMultiply}
import shredding.wmcc._
import shredding.utils.Utils.ind

class SparkDatasetGenerator(cache: Boolean, evaluate: Boolean, skew: Boolean = false, isDict: Boolean = true,
  unshred: Boolean = false, evalFinal: Boolean = true, inputs: Map[Type, String] = Map()) extends SparkTypeHandler with SparkUtils {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs
  var encoders: Set[String] = Set()
  override val bagtype: String = "Seq"

  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }

  def generateEncoders(): String = encoders.map{
    case r => s"implicit val encoder$r: Encoder[$r] = Encoders.product[$r]"
  }.mkString("\n")

  def drop(tp: Type, v: Variable, field: String, index: Boolean = true): CExpr = tp match {
    case RecordCType(fs) => 
      val imap = if (index) Map("index" -> Index) else Map()
      Record(imap ++ (fs - field).map{ case (
        attr, atp) => attr -> Project(v, attr)})
    case _ => sys.error(s"unsupported type ${tp}")
  }

  def generateJoin(e1: CExpr, e2: CExpr, p1: String, p2: String, v1: List[Variable], v2: Variable, joinType: String = "inner"): String = {
    val wrapOption = if (joinType != "inner") "null" else ""
    val recTp = flatRecord(flatType(v1, !isDict, wrapOption), e2.tp, {joinType != "inner"})
    handleType(recTp)
    val rec = generateType(recTp)
    val ge1 = generate(e1)
    val ge2 = generate(e2)
    val gtp1 = generateType(v2.tp)
    val gtp2 = if (skew) ", Int" else ""
    s"""|$ge1.equiJoin[$gtp1$gtp2]($ge2, Seq("$p1","$p2"), "$joinType")
        | .as[$rec]
        |""".stripMargin     
  }
  
  def vars(vs: List[Variable]): String = {
    if (vs.size == 1) generate(vs.head)
    else vs.map(generate(_)).mkString("(", ",", ")")
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
    // {
    //   val tp = e.tp
    //   handleType(tp)
    //   val inner = fs.map{f => generate(f._2)}.mkString(", ")
    //   s"${generateType(tp)}($inner)"
    // }
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

    case FlatDict(e1) => s"${generate(e1)}"
    case GroupDict(e1) => generate(e1) 

    case Nest(e1, v1, Tuple(fs), e2 @ Record(ms), v2, p, Tuple(gs), dk) => 
      val fv1Tp = flatType(v1, !isDict, wrapOption = "null")
      handleType(fv1Tp)
      val fv1 = Variable.fresh(fv1Tp)
      val gv1 = generate(fv1)
      val key = Record(flatType(fs.asInstanceOf[List[Variable]]).attrTps.map{
        case (attr, expr) => attr -> Project(fv1, attr)})
      handleType(key.tp)
      val kv1 = Variable.fresh(key.tp)
      val gkv1 = generate(kv1)
      val gkey = key.fields.map{
        case (attr, Project(sv, attr2)) => s"$gv1._1.$attr2"
        case (attr, expr) => generate(expr)
      }.mkString(s"${generateType(key.tp)}(", ",", ")")
      handleType(e2.tp)
      val tops = ms.filter(_._2 match { case v:Variable => false; case _ => true})
      val grecFull = ms.map{
        case (attr, Variable(n, rtp @ RecordCType(rs))) => 
          encoders = encoders + generateType(rtp)
          rs.map{ case r => s"$gv1._2.${r._1} match { case Some(v) => v; case _ => ${castNull(r._2)} }" }.mkString(s"${generateType(rtp)}(",",",")")
        case (attr, Variable(n, BagCType(rtp @ RecordCType(rs)))) => 
          encoders = encoders + generateType(rtp)
          val srec = rs.map{ case r => s"$n.${r._1}" }.mkString(s"${generateType(rtp)}(",",",")")
          s"$gv1._2.map($n => $srec)"
        case (attr, Project(_, attr2)) => s"$gv1._1.$attr2.get"
      }.mkString(s"Seq(${generateType(e2.tp)}(",",","))")
      encoders = encoders + generateType(e2.tp)
      s"""|${generate(e1)}
          | .groupByKey($gv1 =>
          |   $gkey).mapGroups{
          |   case ($gkv1, group) => 
          |     val ngroup = group.flatMap{
          |       $gv1 => $gv1${getIndex(v1, tops.head._1)}.${tops.head._1} match {
          |         case None => Seq()
          |         case _ => $grecFull
          |        }
          |     }.toSeq
          |   ($gkv1, ngroup)
          |}
          |""".stripMargin          

    case Bind(rv, Reduce(e1, v1, f1, Constant(nulls)), 
      Bind(cv, CReduceBy(e2, v2, keys, values), e3)) =>
      val ftp = flatType(List(v2))
      val nv2 = Variable.fresh(flatType(v1, (v1.size > 1 && !isDict), wrapOption = nulls.toString))
      handleType(nv2.tp)
      val keyTps = ftp.attrTps.filter{case (attr, tp) => keys.contains(attr)}
      val flatKeys = keyTps get "_1" match {
        case Some(t) => getTypeMap(t) ++ (keyTps - "_1")
        case _ => keyTps
        }
      val fkey = Record(flatKeys.map{case (attr, tp) => attr -> Project(nv2, attr)})
      val agg = "x" + Variable.newId()
      val valueExpr = if (isDict) Variable(agg, ftp.attrTps(values.head)) 
        else COption(Variable(agg, ftp.attrTps(values.head)))
      val vrecOption = Record(fkey.fields.filter(f => keys.contains(f._1))
       + (values.head -> valueExpr))
      val gvrec = generate(vrecOption)
      encoders = encoders + generateType(vrecOption.tp)
      fkey.fields.filter(f => !keys.contains(f._1)) - values.head match {
        case y if y.isEmpty => 
          s"""|val ${generate(cv)} = ${generate(e1)}.groupByKey(${generate(nv2)} => 
              | ${generate(fkey)}).agg(
              |   typed.sum[${generateType(nv2.tp)}](x => x.${values.head})).mapPartitions(
              |     it => it.map{ case (${generate(nv2)}, $agg) => $gvrec })
              |${generate(e3)}
              |""".stripMargin
        case grecMap =>
          s"""|val ${generate(cv)} = ${generate(e1)}.groupByKey(${generate(nv2)} => 
              | ${generate(fkey)}).agg(
              |   typed.sum[${generateType(nv2.tp)}](
              |     x => x.${values.head} match {
              |       case Some(r) => r; case _ => ${zero(ftp.attrTps(values.head))}
              |   })).mapPartitions(
              |     it => it.map{ case (${generate(nv2)}, $agg) => (${generate(Record(grecMap))}, $gvrec)
              | })
              |${generate(e3)}
              |""".stripMargin
      }

    // shouldn't be called by shredded evaluation
    case OuterUnnest(e1, v1s, p1 @ Project(_, f), v2, p, value) => 
      val v1 = Variable.fresh(flatType(v1s, index = true, wrapOption = {if (v1s.size > 1) f else ""}))
      val path = generate(Project(v1, f))
      handleType(v1.tp)
      val nv2 = Variable.fresh(value.tp)
      val frec = unnestDataframe(v1, nv2, f)
      val gfrec = generate(frec)
      val nrec = unnestDataframe(v1, nv2, f, true)
      val gnrec = s"${generateType(frec.tp)}(${nrec.fields.map(f => generate(f._2)).mkString(", ")})"
      if (v1s.size == 1){
        s"""|${generate(e1)}.withColumn("index", monotonically_increasing_id())
            | .as[${generateType(v1.tp)}].flatMap{
            |   case ${generate(v1)} => if ($path.isEmpty) Seq($gnrec) 
            |     else $path.map( ${generate(nv2)} => $gfrec )
            |}.as[${generateType(frec.tp)}]
            |""".stripMargin
      }else{
        s"""|${generate(e1)}.withColumn("index", monotonically_increasing_id())
            | .as[${generateType(v1.tp)}].flatMap{
            |   case ${generate(v1)} => $path match {
            |     case None => Seq($gnrec)
            |     case Some(bag) if bag.isEmpty => Seq($gnrec)
            |     case Some(bag) => bag.map( ${generate(nv2)} => $gfrec )
            | }}.as[${generateType(frec.tp)}]
            |""".stripMargin
      }

    case OuterJoin(e1, e2, v1, Project(_, p1), v2, Project(_, p2), proj1, proj2) => 
      generateJoin(e1, e2, p1, p2, v1, v2, "left_outer")

    case Join(e1, e2, v1, Project(_, p1), v2, Project(_, p2), proj1, proj2) => 
      generateJoin(e1, e2, p1, p2, v1, v2)

    case Bind(_, CoGroup(e1, e2, v1, v2, k1 @ Project(pv1, f1), k2, value),
      Bind(rv, Reduce(re1, v, Record(ms), Constant(true)), e3)) => 
      val gv2 = generate(v2)
      val gv2Rec = generate(value)
      val ge2 = s"${generate(e2)}.groupByKey($gv2 => ${generate(k2)})"
      val ve1 = "x" + Variable.newId()
      val ve2 = "x" + Variable.newId()
      val ve3 = "x" + Variable.newId()
      val nrec = Record(ms.map{
        case (attr, value) => value match {
          case tv:Variable => (attr, Variable(ve3, tv.tp))
          case _ => (attr, value)
        }
      })
      encoders = encoders + generateType(value.tp)
      s"""|val ${generate(rv)} = ${generate(e1)}.cogroup($ge2, ${generate(pv1)} => ${generate(k1)})(
          |   (_, $ve1, $ve2) => {
          |     val $ve3 = $ve2.map($gv2 => $gv2Rec).toSeq
          |     $ve1.map(${generate(v.head)} => ${generate(nrec)})
          | }).as[${generateType(nrec.tp)}]
          |${generate(e3)}
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
      encoders = encoders + generateType(nrec.tp)
      s"""|val $glv = ${generate(e1)}.cogroup($ge2.groupByKey(x => x._1), ${generate(v1)} => ${generate(p1)})(
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
    case Reduce(e1, v, f @ Record(fs), Constant(true)) if v.size > 1 =>
      val fTp = wrapOption(f.tp)
      handleType(fTp)
      val rec = s"${generateType(fTp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
      val v1s = vars(v)
      s"""|${generate(e1)}.mapPartitions{ it => it.map{
          | case $v1s => $rec
          |}}.as[${generateType(fTp)}]
          |""".stripMargin

    /** PROJECT **/
    case Reduce(e1, v, f @ Record(_), Constant(true)) =>
      val frec = if (isDict) flattenLabel(f) else f
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
      // fs.map(generate(_)+"/** this **/").mkString("\n")
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"
    case _ => s"/** TODO: $e **/"
  }

}