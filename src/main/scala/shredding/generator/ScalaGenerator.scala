package shredding.generator

import shredding.core._
import shredding.wmcc._
import shredding.utils.Utils.ind

/**
  * Generates Scala code, assumes nodes are compiled from ANF
  */

class ScalaNamedGenerator(inputs: Map[Type, String] = Map()) {
  var types:Map[Type,String] = inputs
  var typelst:Seq[Type] = Seq()//inputs.map(_._1).toSeq

  implicit def expToString(e: CExpr): String = generate(e)
  
  def kvName(x: String): String = x match {
    case "k" => "_1"
    case "v" => "_2" 
    case _ => x
  } 

  def generateTypeDef(tp: Type): String = tp match {
    case RecordCType(fs) =>
     val name = types(tp)
      s"case class $name(${fs.map(x => s"${kvName(x._1)}: ${generateType(x._2)}").mkString(", ")})" 
    case _ => sys.error("unsupported type "+tp)
  }

  def generateType(tp: Type): String = tp match {
    case RecordCType(_) if types.contains(tp) => types(tp)
    case IntType => "Int"
    case StringType => "String"
    case BoolType => "Boolean"
    case DoubleType => "Double"
    case TTupleType(fs) => s"${fs.map(generateType(_)).mkString(",")})"
    case BagCType(tp) => s"List[${generateType(tp)}]" 
    case BagDictCType(flat @ BagCType(TTupleType(fs)), dict) => 
      dict match {
        case TupleDictCType(ds) if !ds.filter(_._2 != EmptyDictCType).isEmpty => 
          s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], ${generateType(dict)})"
        case _ => s"(List[(${generateType(fs.head)}, ${generateType(fs.last)})], Unit)"
      }
    case TupleDictCType(fs) if !fs.filter(_._2 != EmptyDictCType).isEmpty => 
      generateType(RecordCType(fs.filter(_._2 != EmptyDictCType)))
    case LabelType(fs) if fs.isEmpty => "Int" 
    case LabelType(fs) => generateType(RecordCType(fs))
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
          handleType(fs.head, Some(givenName.getOrElse("")+s"Label$nid"))
          handleType(fs.last, Some(givenName.getOrElse("")+s"Flat$nid"))
          handleType(dict, Some(givenName.getOrElse("")+s"Dict$nid"))
        case TupleDictCType(fs) =>
          val ffs = fs.filter(_._2 != EmptyDictCType)
          if (!ffs.isEmpty) { handleType(RecordCType(ffs), givenName) } else { () }
        case _ => ()
      }
    }
  }
  
  def conditional(p: CExpr, thenp: String, elsep: String): String = p match {
    case Constant(true) => s"${ind(thenp)}"
    case _ => s"if({${generate(p)}}) {${ind(thenp)}} else {${ind(elsep)}}"
  }

  def generate(e: CExpr): String = e match {
    case Variable(name, _) => name
    case InputRef(name, tp) => 
      handleType(tp, Some("Input_"+name))
      name
    case Comprehension(e1, v, p, e) =>
      val acc = "acc" + Variable.newId()
      val cur = generate(v)
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
    case Project(e, field) => e.tp match {
      case BagDictCType(_,_) if (field == "lbl") => s"${generate(e)}._1"
      case BagDictCType(_,_) if (field == "flat") => s"${generate(e)}._1"
      case _ => s"${generate(e)}.${kvName(field)}"
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
    case If(cond, e1, e2) => e2 match {
      case Some(a) => s"""
        | if (${cond}) {
        | ${ind(e1)})
        | } else {
        | ${ind(a)}
        | }""".stripMargin
      case None => s"""
        | if (${cond})
        | ${ind(e1)}
        | else  Nil """.stripMargin
    }
    case Merge(e1, e2) => s"${generate(e1) ++ generate(e2)}"
    case CDeDup(e1) => s"${generate(e1)}.distinct"
    case Bind(x, CNamed(n, e), e2) => s"val $n = ${generate(e)}\nval ${generate(x)} = $n\n${generate(e2)}"
    case LinearCSet(exprs) => 
      s"""(${exprs.map(generate(_)).mkString(",")})"""
    case EmptyCDict => "()"
    case BagCDict(lbl, flat, dict) => 
      s"(${generate(flat)}.map(v => (${generate(lbl)}, v)), ${generate(dict)})"
    case TupleCDict(fs) => generate(Record(fs))
    case Select(x, v, p) => p match {
      case Constant(true) => generate(x)
      case _ => s"${generate(x)}.filter(${generate(v)} => ${generate(p)})"
    }
    case Reduce(e1, v, f, p) => 
      s"${generate(e1)}.map{ case ${generateVars(v, e1.tp.asInstanceOf[BagCType].tp)} => { \n${ind(generate(f))} }}"
    case Unnest(e1, v1, f, v2, p) => 
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val nv = "nv"+Variable.newId()
      p match {
        case Constant(true) =>
          s"""|${generate(e1)}.flatMap{ case $vars => 
              |${ind(generate(f))}.map($gv2 => {
              |${ind(s"val $nv = ($vars, $gv2) \n ${conditional(p, nv, "Nil")}")}
              |})}""".stripMargin
        case _ => // ask about this
          s"""|${generate(e1)}.flatMap{ case $vars =>
              |${ind(generate(f))}.withFilter{ case $gv2 =>
              |${ind(s"{${generate(p)}}}.map($gv2 => ($vars, $gv2))")}
              |}""".stripMargin
        /**case _ => // doesn't preserve type
          s"""|${generate(e1)}.flatMap{ case $vars => 
            |${ind(generate(f))}.collect{ case $gv2 =>
            |${ind(s"if({${generate(p)}}) ($vars, $gv2)")}
            |}}""".stripMargin**/
      }
    case Nest(e1, v1, f, e2, v2, p) =>
      val grps = "grps" + Variable.newId()
      val acc = "acc"+Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 = generate(v2)
      val grped = s"{ val $grps = ${generate(e1)}.groupBy{ case $vars => { ${generate(f)} }}"
      e2.tp match {
        case IntType => 
          s"$grped\n $grps.map($gv2 => ($gv2._1, $gv2._2.foldLeft(0)(($acc, $gv2) => $acc + ${generate(e2)}))).toList }"  
        case _ => s"$grped\n $grps.map($gv2 => ($gv2._1, $gv2._2.map{case $vars => ${generate(e2)}})).toList }"
      }
    case Lookup(e1, e2, v1, p1, v2, p2, p3) => 
      val hm = "hm" + Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      val gv2 =  generate(v2)
      val filt = p3 match {
        case Constant(true) => s""
        case _ => s".withFilter($gv2 => { ${generate(p3)} } )"
      }
      s"""|{ val $hm = ${generate(e2)}.toMap
          | ${generate(e1)}.flatMap{case $vars => $hm.get({ ${generate(p1)} }) match {
          | case Some(a) => a$filt.map($gv2 => ($vars, $gv2))
          | case _ => Nil
          |}}}""".stripMargin
      
      /**s"""|{ val $hm = ${generate(e1)}.groupBy{ case $vars => { ${generate(p1)} } }
          | ${generate(e2)}.flatMap(v1 => v1._2.map($gv2 =>
          | $hm.get({${generate(Bind(v3, Project(v4, "_1"), jcond2))}}) match {
          | case Some(a) => (a.head, $gv2)
          | case _ => (a.head, None)
          | })}""".stripMargin**/
      /**val grpby = jcond2 match {
        case Bind(_, _, Bind(_, Tuple(_), _)) => 
          s"val $hm = ${generate(e2)}.flatMap(v1 => v1._2.map($gv2 => ({${generate(Bind(v3, Project(v4, "_1"), jcond2))}}, v1._2))).toMap"
        case _ => s"val $hm = ${generate(e2)}.toMap"
      }
      s"""|{ $grpby
          |${generate(e1)}.flatMap{ case $vars => $hm.get({${generate(p1)}}) match {
          | case Some(a) => a.map(v1 => ($vars, v1))
          | case _ => Nil
          |}}}""".stripMargin**/
    case Join(e1, e2, v1, p1, v2, p2) =>
      val hm = "hm" + Variable.newId()
      val vars = generateVars(v1, e1.tp.asInstanceOf[BagCType].tp)
      s"""|{ val $hm = ${generate(e1)}.groupBy{ case $vars => {
        |${ind(generate(p1))}}}
        |${generate(e2)}.flatMap(${generate(v2)} => $hm.get({${generate(p2)}}) match {
        | case Some(a) => a.map(v => (v, ${generate(v2)}))
        | case _ => Nil
        |}) }""".stripMargin
    case OuterJoin(e1, e2, v1, p1, v2, p2) => generate(Join(e1, e2, v1, p1, v2, p2))
    case OuterUnnest(e1, v1, f, v2, p) => generate(Unnest(e1, v1, f, v2, p))
    case Bind(v, e1, e2) =>
      s"val ${generate(v)} = ${generate(e1)}\n${generate(e2)}"
    case _ => sys.error("not supported "+e)
  }

  def toList(e: String): String = e match {
    case _ if e.startsWith("(") => s"List$e"
    case _ => s"List($e)"
  }

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

}
