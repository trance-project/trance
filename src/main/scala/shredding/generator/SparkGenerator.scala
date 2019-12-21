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
    case "key" if s == 2 => "_1"
    case "value" if s == 2 => "_2"
    case _ => x
  }

  def generateTypeDef(tp: Type): String = tp match {
    case LabelType(fs) =>
     val name = types(tp)
     val fsize = fs.size
      s"case class $name(${fs.map(x => s"${kvName(x._1)(fsize)}: ${generateType(x._2)}").mkString(", ")})"
    case RecordCType(fs) =>
     val name = types(tp)
     val fsize = fs.size
      s"case class $name(${fs.map(x => s"${kvName(x._1)(fsize)}: ${generateType(x._2)}").mkString(", ")})"
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(fs) if fs.isEmpty => "Unit"
    case LabelType(fs) if fs.isEmpty => "Unit"
    //case RecordCType(fs) if fs.keySet == Set("_1", "_2") => fs.generateType(fs._2).mkString("(",",",")")
    case RecordCType(_) if types.contains(tp) => types(tp)
    case LabelType(fs) => generateType(RecordCType(fs))
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
        case LabelType(fs) if !fs.isEmpty => handleType(RecordCType(fs))
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
    case Record(fs) if (fs.keySet == Set("_1","_2") || fs.keySet == Set("key", "value")) => // fix this earlier in pipeline
      s"""(${fs.map(f => { handleType(f._2.tp); generate(f._2) } ).mkString(", ") })"""
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
              s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs(i-1))},${fs.slice(i, fs.size).map(e => "_").mkString(",")}) => Nil" //({${generate(f)}}, ${zero(e2)})"
            } else { 
              s"case (${fs.slice(1, i).map(e => "_").mkString(",")},${zero(fs.last)}) => Nil" //({${generate(f)}}, ${zero(e2)})" 
            }
          ).mkString("\n")
		case _ => s"case (null) => Nil" //({${generate(f)}}, ${zero(e2)})"
      }
      val e2tp = e2.tp match {
        case IntType => false
        case DoubleType => false
        case _ => true
      }
      def isLabel(tp: Type): Boolean = tp match {
        case TTupleType(fs) => isLabel(fs.head)
        case RecordCType(fs) if fs.keySet == Set("lbl") => true
        case LabelType(fs) => true
        case _ => false
      } 
      val gbfun = if (isLabel(f.tp)) ".groupByLabel()" else ".groupByKey()"
      //if (shredded) ".groupByLabel()" else ".groupByKey()"
      p match {
        case Constant(true) if e2tp => 
          s"""|${generate(e1)}.flatMap{ case $vars => ${generate(g)} match {
              |   $nonet 
              |   case $gv2 => List(({${generate(f)}}, {${generate(e2)}}))
              | }
              |}$gbfun""".stripMargin
        case Constant(true) => g match {
          case _ =>
            println(s"in here with ${e2.tp}")
            s"""|${generate(e1)}.flatMap{ case $vars => ${generate(g)} match {
                |   $nonet
                |   case $gv2 => List(({${generate(f)}}, {${generate(e2)}}))
                | }
                |}.reduceByKey(_ + _)""".stripMargin
        }
        case y if e2tp => 
          s"""|${generate(e1)}.flatMap{ case $vars => ${generate(g)} match {
              |   case $gv2 if {${generate(p)}} => List(({${generate(f)}}, {${generate(e2)}}))
              |   case $gv2 => List(({${generate(f)}}, ${zero(e2)}))
              | }    
              |}$gbfun""".stripMargin
        case _ =>
          s"""|${generate(e1)}.flatMap{ case $vars => ${generate(g)} match {
              |   case $gv2 if {${generate(p)}} => List(({${generate(f)}}, {${generate(e2)}}))
              |   case $gv2 => List(({${generate(f)}}, ${zero(e2)}))
              | }
              |}.reduceByKey(_ + _)""".stripMargin
      }
    // experimental unnesting that is closer to the functionality of flatmap
    // since it is a dictionary we do not need the outer functionality
    case Unnest(e1 @ Variable(_, BagDictCType(flat, dict)), v1, f, v2, p) => 
      val vars = generateVars(v1, flat.tp)
      val gv2 = generate(v2)
      val filt = p match {
        case Constant(true) => s"($vars, $gv2)"
        case _ => s"if ({${generate(p)}}) { ($vars, $gv2) } else { ($vars, null) }"
      }
      s"""|${generate(e1)}.flatMap{ 
          | case $vars => {${generate(f)}}.map{ case v2 => ($vars._1, v2) }
          |}
        """.stripMargin
    case Unnest(e1, v1, f, v2, p) => 
      val vars = generateVars(v1, e1.tp match {
        case btp:BagDictCType => btp.flatTp.tp
        case btp:BagCType => btp.tp
      })
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
    case OuterUnnest(e1 @ Variable(_, BagDictCType(flat, dict)), v1, f, v2, p) => generate(Unnest(e1, v1, f, v2, p)) 
    case OuterUnnest(e1, v1, f, v2, p) => 
      println(e1)
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
      val vars = e1.tp match {
        case btp:BagCType => generateVars(v1, btp.tp)
        case BagDictCType(flat, tdict) => generateVars(v1, flat.tp)
      }
      val gv2 = generate(v2)
      (p1, p2) match {
        case (Constant(true), Constant(true)) =>
          s"${generate(e1)}.cartesian(${generate(e2)})"
        case _ => 
      // ${checkNull(v1)} in map below causes invariance issues in PairRDDFunctions
       s"""|{ val out1 = ${generate(e1)}.map{ case $vars => ({${generate(p1)}}, $vars) }
           |  val out2 = ${generate(e2)}.map{ case $gv2 => ({${generate(p2)}}, $gv2) }
           |  out1.join(out2).map{ case (k,v) => v }
           |}""".stripMargin
      }
    // this only handles the case where there is no secondary join
    // for instance, ( Mctx2 join top level dict ) join first level dict
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val flatten = e2 match { case InputRef(n, _) if !n.contains("new") => ".flatten"; case _ => "" }
      s"""|{ val out1 = ${generate(e1)}.map{ case $vars => (${e1Key(p1, Constant(true))}, $vars) }
          |out1.cogroup(${generate(e2)}).flatMap{
          | case (_, (left, $gv2)) => left.map{ case $vars => ($vars, $gv2$flatten) }
          |}}
        """.stripMargin
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) => generate(Lookup(e1, e2, v1, p1, v2, p2, p3))
    case CoGroup(e1, es, vs, ps) => /** TODO THIS IS NOT DONE **/
      s"""|{ val out1 = ${generate(e1)}.map{ case v => (${generate(ps)}, v) }
          | out.cogroup(${es.map{e2 => generate(e2)}.mkString(",")})
          |}""".stripMargin
    case Select(x, v, p, e2) => 
	  val gv = generate(v)
      val proj = e2 match {
        case Variable(_,_) => ""
        case InputRef(_,_) => ""
        case _ => s".map($gv => { ${generate(e2)} })"
      }
   	  p match {
        case Constant(true) => s"${generate(x)}$proj"
        case _ => s"${generate(x)}.filter($gv => { ${generate(p)} })$proj"
      }
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
    case Bind(x, CNamed(n, e1), e2) =>
    	s"""|val $n = ${generate(e1)}
            |val ${generate(x)} = $n
            |//$n.collect.foreach(println(_))
            |${generate(e2)}""".stripMargin
    case Bind(x, e1 @ LinearCSet(rs), e2) =>
      // count will be called on the last item in the linear set
      s"${generate(rs.last)}"
    case Bind(v, e1, e2) => 
      s"val ${generate(v)} = ${generate(e1)} \n${generate(e2)}"
    case _ => sys.error(s"not supported $e")
  }

  def validLabel(e: Type): Boolean = e match {
    case EmptyCType => true
    case LabelType(_) => true
    case _ => false
  }

  /**
    * Tuple vars based on type, for example (a,b,c) -> ((a,b),c)
    */
  def generateVars(e: List[Variable], tp: Type): String = tp match {
    case TTupleType(seq) if (seq.size == 2 && validLabel(seq.head)) => s"${generate(e.head)}"
    case TTupleType(seq) if e.size == seq.size => e.map(generate).mkString("(", ", ", ")")
    case TTupleType(seq) if e.size > seq.size => {
      val en = e.dropRight(seq.size - 1)
      val rest = e.takeRight(seq.size - 1).map(generate).mkString(", ")
      s"(${generateVars(en, seq.head)}, $rest)"
    }
    case y if e.size == 1 => s"${generate(e.head)}" 
    //TTupleType(seq) if seq.size == 2 && e.size == 1 => e.map(generate).mkString("(", ",", ")")
    case TTupleType(seq) => sys.error(s"not supported ${e.size} ${seq.size} --> $e:\n ")//${generateType(tp)}")
    //case _ if e.size == 1 => s"${generate(e.head)}"
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
      case Constant(true) => s".flatMapValues(identity)"
      case _ => 
      s"""
        /** WHEN DOES THIS CASE HAPPEN **/
        .flatMap(v2 => v2._2.map{case $gv2 => ((v2._1, {${generate(p2)}}), $gv2)})"""
    }
  }


}
