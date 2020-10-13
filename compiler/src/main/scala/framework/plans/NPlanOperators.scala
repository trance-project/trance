package framework.plans

import framework.common._
import framework.utils._
import framework.nrc.{NRC, MaterializeNRC}
import framework.nrc.{Label => NRCLabel}
import framework.nrc.{Printer => NRCPrinter}

trait PlanOperator extends NRC with NRCLabel with MaterializeNRC with NRCPrinter {

  // val nrc = new NRC{}
  // import nrc._

  trait PExpr {
    def tp: Type 
  }

  case class Plan(name: String, plan: PExpr) extends PExpr {
    def tp: Type = plan.tp
  }

  case class PlanSet(plans: List[PExpr]) extends PExpr {
    def tp: Type = BagType(TupleType(Map.empty[String, TupleAttributeType]))
  }

  case class Select(in: Expr, v: VarDef, p: Expr) extends PExpr {
    def tp: BagType = in.tp.asInstanceOf[BagType]
    override def toString: String = p match {
      case c:Const if c.v.asInstanceOf[Boolean] => s"[${quote(in)}]"
      case _ => s"""|[\\sigma^{${quote(p)}}
                    |   ${quote(in)}]""".stripMargin
    }
  }

  case class Index(in: Expr, name: String) extends PExpr {
    def tp: BagType = {
      val ms = (in.tp.attrs ++ Map(name -> LongType)).asInstanceOf[Map[String, TupleAttributeType]]
      BagType(TupleType(ms))
    }
    override def toString: String = s"Index(${quote(in)})"
  }

  case class Projection(in: PExpr, v: VarDef, t: Expr) extends PExpr {
    def tp: BagType = t.tp match {
      case ttp:TupleType => BagType(ttp)
      case ttp:BagType => ttp
      case ttp => sys.error(s"unsupported type $ttp")
    }
    override def toString: String = 
      s"""[\\pi_{${quote(t)}}
          |   $in]""".stripMargin
  }

  case class OuterJoin(lin: PExpr, lv: VarDef, rin: PExpr, rv: VarDef, cond: Expr, fields: Set[String]) extends PExpr {
    def tp: BagType = BagType(lv.tp.merge2(rv.tp.outer2).project(fields))
    override def toString: String = 
      s"""|[\\ojoin_{${quote(cond)}}^{${fields.toList.mkString("(", ",", ")")}}
          |   $rin
          |   $lin]""".stripMargin
  }

  case class Nest(in: PExpr, v: VarDef, key: Set[String], value: Expr, filter: Expr, nulls: Set[String]) extends PExpr {
    def tp: BagType = value match {
      case _:NumericType => 
        BagType(TupleType((in.tp.attrs.filter(f => key(f._1)) ++ 
          Map("_2" -> DoubleType)).asInstanceOf[Map[String, TupleAttributeType]]))
      case _ => 
        BagType(TupleType((in.tp.attrs.filter(f => key(f._1)) ++ 
          Map("_2" -> BagType(value.tp.unouter2))).asInstanceOf[Map[String, TupleAttributeType]]))
    }
    override def toString: String = 
      s"""|[\\gamma_{${quote(value).replace("\n", "")}}^{ U / ${key.toList.mkString("(", ",", ")")}}
          |    $in]""".stripMargin
  }

}