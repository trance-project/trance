package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.utils.Utils.ind

/**
  * Generates Scala code specific to Spark applications
  */

class SparkNamedGenerator(inputs: Map[Type, String] = Map()) {

  implicit def expToString(e: CExpr): String = generate(e)

  var types: Map[Type, String] = inputs
  var typelst: Seq[Type] = Seq()

  def kvName(x: String)(implicit s: Int = -1): String = x match {
    case "k" if s == 2 => "_1"
    case "v" if s == 2 => "_2"
    case _ => x
  }

  def generateTypeDef(tp: Type): String = tp match {
    case RecordCType(fs) =>
     val name = types(tp)
     val fsize = fs.size
      s"case class $name(${fs.map(x => s"${kvName(x._1)(fsize)}: ${generateType(x._2)}").mkString(", ")})"
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(_) if types.contains(tp) => types(tp)
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case DoubleType => "Double"
    case LongType => "Long"
    case TTupleType(fs) => s"(${fs.map(generateType(_)).mkString(",")})"
    case BagCType(tp) => s"Iterable[${generateType(tp)}]" //combineByKey, etc. may need this to be iterable
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
      dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty =>
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty =>
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
    case RecordCType(fs) if fs.isEmpty => "Unit"
    //case LabelType(fs) => generateType(RecordCType(fs))
    case EmptyCType => "Unit"
    case _ => sys.error("not supported type " + tp)
  }

  def generateHeader(names: List[String] = List()): String = {
    val h1 = typelst.map(x => generateTypeDef(x)).mkString("\n")
    val h2 = inputs.withFilter(x => !names.contains(x._2)).map( x => generateTypeDef(x._1)).toList
    if (h2.nonEmpty) { s"$h1\n${h2.mkString("\n")}" } else { h1 }
  }
  
  def handleType(tp: Type, givenName: Option[String] = None): Unit = {
    if(!types.contains(tp)) {
      tp match {
        case RecordCType(fs) =>
          fs.foreach(f => handleType(f._2))
          val name = givenName.getOrElse("Record" + Variable.newId)
          types = types + (tp -> name)
          typelst = typelst :+ tp
        case BagCType(tp) =>
          handleType(tp, givenName)
        case LabelType(fs) if !fs.isEmpty =>
          val name = givenName.getOrElse("Label" + Variable.newId)
          handleType(RecordCType(fs), Some(name))
        case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) =>
          val nid = Variable.newId
          handleType(fs.last, Some(givenName.getOrElse("")))
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs), givenName) } else { () }
        case _ => ()
      }
    }
  }

  def generate(e: CExpr): String = e match {
    case Variable(name, _) => name
    case InputRef(name, tp) => name
    case Comprehension(e1, v, p, e) =>
        val acc = "acc" + Variable.newId()
        val cur = generate(v)
        def conditional(p: CExpr, thenp: String, elsep: String): String = p match {
          case Constant(true) => s"${ind(thenp)}"
          case _ => s"if({${generate(p)}}) {${ind(thenp)}} else {${ind(elsep)}}"
        }
        e.tp match {
          case IntType =>
            s"${generate(e1)}.foldLeft(0)(($acc, $cur) => \n${ind(conditional(p, s"$acc + {${generate(e)}}", s"$acc"))})"
          case DoubleType =>
            s"${generate(e1)}.foldLeft(0.0)(($acc, $cur) => \n${ind(conditional(p, s"$acc + {${generate(e)}}", s"$acc"))})"
          case _ =>
            s"${generate(e1)}.flatMap($cur => { \n${ind(conditional(p, s"${generate(e)}", "Nil"))}})"
        }
    case Record(fs) => {
      val tp = e.tp
      handleType(tp)
      s"${generateType(tp)}(${fs.map(f => generate(f._2)).mkString(", ")})"
    }
    case Tuple(fs) => s"(${fs.map(f => generate(f)).mkString(",")})"
    case Project(e2, field) => e2.tp match {
      case BagDictCType(_,_) => s"${generate(e2)}${field.replace(".", "")}"
      case TupleDictCType(_) => s"${generate(e2)}${field.replace(".", "")}"
      case TTupleType(List(IntType, RecordCType(_))) if field != "_2" => s"${generate(e2)}._2.${kvName(field)}"
      case RecordCType(fs) => s"${generate(e2)}.${kvName(field)(fs.size)}"
      case _ => s"${generate(e2)}.${kvName(field)}"
    }
    case Equals(e1, e2) => s"${generate(e1)} == ${generate(e2)}"
    case Lt(e1, e2) => s"${generate(e1)} < ${generate(e2)}"
    case Gt(e1, e2) => s"${generate(e1)} > ${generate(e2)}"
    case Lte(e1, e2) => s"${generate(e1)} <= ${generate(e2)}"
    case Gte(e1, e2) => s"${generate(e1)} >= ${generate(e2)}"
    case And(e1, e2) => s"${generate(e1)} && ${generate(e2)}"
    case Or(e1, e2) => s"${generate(e1)} || ${generate(e2)}"
    case Not(e1) => s"!(${generate(e1)})"
    case Constant(x) => x match {
      case s:String => "\"" + s + "\""
      case _ => x.toString
    }
    case Sng(e) => s"List(${generate(e)})"
    case WeightedSng(e, q) => s"(1 to ${generate(q)}.asInstanceOf[Int]).map(v => ${generate(e)})"
    case CUnit => "()"
    case EmptySng => "Nil"
    case If(cond, e1, Some(e2)) => s"""
        | if ({${generate(cond)}}) {
        | {${ind(generate(e1))}}
        | } else {
        | {${ind(generate(e2))}}
        | }""".stripMargin
    case If(cond, e1, None) => 
      val zero = e1.tp match {
        case IntType => "0"
        case DoubleType => "0.0"
        case _ => "Nil"
      } 
	    s"""
        | if ({${generate(cond)}})
        | {${ind(generate(e1))}}
        | else $zero """.stripMargin
    case Merge(e1, e2) => s"${generate(e1) ++ generate(e2)}"
    case CDeDup(e1) => s"${generate(e1)}.distinct"
    case EmptyCDict => s"()"
    case Nest(e1, v1, f, e2, v2, p, g) =>
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val zero = (z: CExpr) => z.tp match {
        case IntType => "0"
        case DoubleType => "0.0"
        case _ => "null"
      } 
      val acc = "acc"+Variable.newId
      val gv2 = generate(v2)
      val nonet = g match {
        case Bind(_, Tuple(fs), _) if fs.size != 1 => 
          (2 to fs.size).map(i => 
            if (i != fs.size) {
              s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs(i-1))},${fs.slice(i, fs.size).map(e => "_").mkString(",")}) => ({${generate(f)}}, ${zero(e2)})"
            } else { 
              s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs.last)}) => ({${generate(f)}}, ${zero(e2)})" 
            }
          ).mkString("\n")
        case _ => s"case (null) => ({${generate(f)}}, ${zero(e2)})"
      }
      
      // for now check if this is the shredded version
      // could move a local option to the nest node
      val gbfun = if (v1.head.tp match {
        case r @ RecordCType(_) => r.attrTps.contains("lbl")
        case _ => false
      }) ".groupByLabel()" 
      else ".groupByKey()"

      (p, e2.tp) match {
        case (Constant(true), RecordCType(_)) => 
          s"""|${generate(e1)}.map{ case $vars => ${generate(g)} match {
              |   $nonet 
              |   case $gv2 => ({${generate(f)}}, {${generate(e2)}})
              | }
              |}$gbfun""".stripMargin
        case (Constant(true), _) => g match {
          case Bind(_, CUnit, _) =>
            s"""|${generate(e1)}.map{ case $vars => 
                |   ({${generate(f)}}, {${generate(e2)}})
                |}.reduceByKey(_ + _)""".stripMargin
          case _ =>
            s"""|${generate(e1)}.map{ case $vars => ${generate(g)} match {
                |   $nonet
                |   case $gv2 => ({${generate(f)}}, {${generate(e2)}})
                | }
                |}.reduceByKey(_ + _)""".stripMargin
        }
        case (_, RecordCType(_)) => 
          s"""|${generate(e1)}.map{ case $vars => ${generate(g)} match {
              |   case $gv2 if {${generate(p)}} => ({${generate(f)}}, {${generate(e2)}})
              |   case $gv2 => ({${generate(f)}}, ${zero(e2)})
              | }    
              |}$gbfun""".stripMargin
        case _ =>
          s"""|${generate(e1)}.map{ case $vars => ${generate(g)} match {
              |   case $gv2 if {${generate(p)}} => ({${generate(f)}}, {${generate(e2)}})
              |   case $gv2 => ({${generate(f)}}, ${zero(e2)})
              | }
              |}.reduceByKey(_ + _)""".stripMargin
      }
    case Unnest(e1, v1, f, v2, p) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val filt = p match {
        case Constant(true) => ""
        case _ => s".filter{ case ($vars, $gv2) => {${generate(p)}} }"
      }
      s"""|${generate(e1)}.flatMap{ case $vars => $vars match {
          |   case ${if (v1.size == 1) { "null" } else { "(_, null)" }} => List(($vars, null))
          |   case _ =>
          |   ${generate(f)} match {
          |     case $gv2 => $gv2.map{ case v2 => ($vars, v2) }
          |  }
          | }}$filt""".stripMargin
    case OuterUnnest(e1, v1, f, v2, p) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val filt = p match {
        case Constant(true) => s"($vars, $gv2)"
        case _ => s"if ({${generate(p)}}) { ($vars, $gv2) } else { ($vars, null) }"
      }
      s"""|${generate(e1)}.flatMap{ case $vars => $vars match {
          |   case ${if (v1.size == 1) { "null" } else { "(_, null)" }} => List(($vars, null))
          |   case _ => 
          |   {${generate(f)}} match {
          |     case Nil => List(($vars, null))
          |     case lst => lst.map{ case $gv2 => $filt }
          |  }
          | }}""".stripMargin
    case OuterJoin(e1, e2, v1, p1, v2, p2) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      // ${checkNull(v1)} in map below causes invariance issues in PairRDDFunctions
      s"""|{ val out1 = ${generate(e1)}.map{ case $vars => ({${generate(p1)}}, $vars) }
          |  val out2 = ${generate(e2)}.map{ case $gv2 => ({${generate(p2)}}, $gv2) }
          |  out1.join(out2).map{ case (k,v) => v }
          |  //out1.leftOuterJoin(out2).map{ case (k, (a, Some(v))) => (a, v); case (k, (a, None)) => (a, null) }
          |}""".stripMargin
    case Join(e1, e2, v1, p1, v2, p2) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      (p1, p2) match {
        case (Constant(true), Constant(true)) =>
          // this will override with actual cartesian product
          s"${generate(e2)}.map{ case c => (${generate(e1)}.head, c) }"
        case _ => 
      // ${checkNull(v1)} in map below causes invariance issues in PairRDDFunctions
       s"""|{ val out1 = ${generate(e1)}.map{ case $vars => ({${generate(p1)}}, $vars) }
           |  val out2 = ${generate(e2)}.map{ case $gv2 => ({${generate(p2)}}, $gv2) }
           |  out1.join(out2).map{ case (k,v) => v }
           |}""".stripMargin
      }
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      s"""|{ val out1 = ${generate(e1)}.map{${checkNull(v1)} case $vars => (${e1Key(p1, p3)}, $vars) }
          |  val out2 = ${generate(e2)}${e2Key(v2, p2)}
          |  out1.join(out2).map{ case (k, v) => v }
          |}""".stripMargin
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) =>      
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      s"""|{ val out1 = ${generate(e1)}.map{${checkNull(v1)} case $vars => (${e1Key(p1, p3)}, $vars) }
          |  val out2 = ${generate(e2)}${e2Key(v2, p2)}
          |  out1.outerLookup(out2)
          |}""".stripMargin
    case Select(x, v, p, e) => 
      val gv = generate(v)
      val proj = e match {
        case Variable(_,_) => ""
        case InputRef(_,_) => ""
        case _ => s".map($gv => { ${generate(e)} })"
      }
      p match {
        case Constant(true) => s"${generate(x)}$proj"
        case _ => s"${generate(x)}.filter($gv => { ${generate(p)} })$proj"
      }
    case Reduce(e1, v, Bind(_, _, Bind(_, Record(fs), _)), p) if fs.keySet == Set("k", "v") && v.last.tp.isInstanceOf[PrimitiveType] =>
      val vars = generateVars(v, e1.tp.asInstanceOf[BagCType].tp)
      s"""|${generate(e1)}.map{ case $vars => 
          |  (${generate(v.head)}, ${generate(Tuple(v.tail))})
          |}.groupByLabel()""".stripMargin
    case Reduce(e1, v, f, p) =>
      val vars = generateVars(v, e1.tp.asInstanceOf[BagCType].tp)
      p match { case Constant(true) =>
          s"""|${generate(e1)}.map{ case $vars => 
              |   ${generate(f)} 
              |}""".stripMargin
        case _ => 
          s"""|${generate(e1)}.map{ case $vars => 
              |   ${generate(f)} 
              |}.filter($vars => {${generate(p)}})""".stripMargin
      }
    case Bind(x, CNamed(n, e), e2) => n match {
       case "M_ctx1" =>
        s"""|val M_ctx1 = ${generate(e)}
            |${generate(e2)}""".stripMargin
      case _ =>
        s"""|val $n = ${generate(e)}
            |//println(\"$n\")
            |val ${generate(x)} = $n
            |//$n.collect.foreach(println(_))
            |${generate(e2)}""".stripMargin
    }
    case Bind(x, e1 @ LinearCSet(rs), e2) =>
      // count will be called on the last item in the linear set
      s"val res = ${generate(rs.last)}"
    case Bind(v, e1, e2) => sanitizeName(e) match {
        case Bind(v2, e3, e4) => s"val ${generate(v2)} = ${generate(e3)} \n${generate(e4)}"
      	case _ => sys.error(s"not support $e")
      }
    case _ => sys.error(s"not supported $e")
  }

  /**
    * Tuple vars based on type, for example (a,b,c) -> ((a,b),c)
    */
  def generateVars(e: List[Variable], tp: Type): String = tp match {
    case TTupleType(seq) if (seq.size == 2 && seq.head == IntType) => s"${generate(e.head)}"
    case TTupleType(seq) if e.size == seq.size => e.map(generate).mkString("(", ", ", ")")
    case TTupleType(seq) if e.size > seq.size => {
      val en = e.dropRight(seq.size - 1)
      val rest = e.takeRight(seq.size - 1).map(generate).mkString(", ")
      s"(${generateVars(en, seq.head)}, $rest)"
    }
    case TTupleType(seq) => sys.error(s"not supported ${e.size} ${seq.size} --> $e:\n ${generateType(tp)}")
    case _ if e.size == 1 => s"${generate(e.head)}"
  }

  
  /**
    * Check for nulls only if there could have been 
    * a previous OUTER operator
    */
  def checkNull(e: List[Variable]) = e.size match {
    case 1 => ""
    case _ => s" case (a, null) => (null, (a, null));" 
  }

  def e1Key(p1: CExpr, p3: CExpr) =  p3 match {
    case Constant(true) => s"{${generate(p1)}}"
    case _ => s"({${generate(p1)}}, {${generate(p3)}})"
  }

  def e2Key(v2: CExpr, p2: CExpr) = {
    val gv2 = generate(v2)
    p2 match {
      // handle this differently for input dictionaries
      case Constant(true) => s".flatMap($gv2 => $gv2._2.map{case v2 => ($gv2._1.lbl, v2)})"
      case _ => s".flatMap(v2 => v2._2.map{case $gv2 => ((v2._1, {${generate(p2)}}), $gv2)})"
    }
  }

  var dictref = Map[Variable, String]()
  /**
    * Truncate a series of dictionary projections into the respective input name in IndicesOf format
    * R__D._1 -> R__D_1, handled by generate(Project(...))
    * R__D._2.field._1 -> R__D_2field_1
    * R__D._2.field._2nextfield._1 -> R__D_2field_2nextfield_1
    */
  def sanitizeName(e: CExpr): CExpr = e match {
    case Bind(v @ Variable(_, TupleDictCType(_)), e1 @ Project(InputRef(dname, _), f), e2) => 
      dictref = dictref ++ Map(v -> s"$dname$f")
      sanitizeName(e2)
    case Bind(v @ Variable(dname, BagDictCType(_,_)), Project(e1 @ Variable(vname, t), f1), Bind(Variable(_, BagCType(_)), Project(e2, f2), Bind(v3, _, e3))) =>
      val nv = dictref.getOrElse(e1, vname)
      if (f2 == "_1") { dictref = dictref ++ Map(v -> s"$nv$f1") }
      sanitizeName(Bind(v, Project(Variable(nv, t), s"$f1$f2"), Bind(v3, v, e3)))
    case Bind(v @ Variable(_, TupleDictCType(_)), Project(e1 @ Variable(vname, t), f1), Bind(Variable(_, BagDictCType(_,_)), Project(e2, f2), Bind(v3, _, e3))) => 
      val nv = dictref.getOrElse(e1, vname)
      sanitizeName(Bind(v, Project(Variable(nv, t), s"$f1$f2"), Bind(v3, v, e3))) 
    case Bind(v @ Variable(_, TupleDictCType(_)), Project(e1 @ Variable(vname, t), f1), Bind(Variable(_, BagCType(_)), Project(e2, f2), Bind(v3, _, e3))) =>
      val nv = dictref.getOrElse(e1, vname)
      sanitizeName(Bind(v, Project(Variable(nv, t), s"$f1$f2"), Bind(v3, v, e3)))
    case _ => e 
  }

}
