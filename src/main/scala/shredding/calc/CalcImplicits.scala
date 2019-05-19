package shredding.calc

import shredding.Utils.ind
import shredding.core._

trait CalcImplicits extends ShredCalc {

  implicit class CompCalcOps(self: CompCalc) {

    def quote: String = self match {
      case Constant(v, _) => "\""+ v +"\""
      case v: Var => v.name
      case Sng(e1) =>
        "{" + e1.quote + "}"
      case Zero() => "{ }"
      case Tup(fields) =>
        s"( ${fields.map(f => f._1 + " := " + f._2.quote).mkString(", ")} )"
      case p:Proj => p.tuple.quote+"."+p.field
      case BagComp(e1, qs) => s"{ ${e1.quote} | ${qs.map(_.quote).mkString(", ")} }"
      case IfStmt(c, e1, e2) => e2 match {
        case Some(e3) => s"""|If (${c.quote})
          |Then ${e1.quote}
          |Else ${e3.quote}""".stripMargin
        case None => s"""|If (${c.quote})
          |Then ${e1.quote}""".stripMargin
      }
      case Conditional(op, e1, e2) => s" ${e1.quote} ${op} ${e2.quote} "
      case NotCondition(e1) => s" not(${e1.quote}) "
      case AndCondition(e1, e2) => s" ${e1.quote} and ${e2.quote} "
      case OrCondition(e1, e2) => s" ${e1.quote} or ${e2.quote} "
      case Merge(e1, e2) => s"{ ${e1.quote} U ${e2.quote} }"
      case BindPrimitive(x, v) => s"${core.quote(x)} := ${v.quote}"
      case BindTuple(x, v) => s"${core.quote(x)} := ${v.quote}"
      case BindLabel(x, v) => s"${core.quote(x)} := ${v.quote}"
      case Generator(x, v) => s" ${core.quote(x)} <- ${v.quote} "
      case CountComp(e1, qs) => s" + { ${e1.quote} | ${qs.map(_.quote).mkString(",")} }"
      case CNamed(v, e1) => v.name+" := "+e1.quote
      case CSequence(exprs) => exprs.map(_.quote).mkString("\n")
      case CLabel(i, vars) => s"Label${i}(${vars.map(v => v._1 -> v._2.quote).toString})"
      case EmptyCDict => "Nil"
      case BagCDict(lbl, flat, dict) => 
            s"""|(${lbl.quote} ->
          |  flat :=
          |${ind(flat.quote, 2)},
          |  tupleDict :=
          |${ind(dict.quote, 2)}
          |)""".stripMargin
      case TupleCDict(fs) => s"(${fs.map { case (k, v) => k + " := " + v.quote }.mkString(", ")})"
      case TupleDictComp(td, bd) => s"{ ${td.quote} | ${bd.quote} }"
      case BagDictProj(dict, field) => dict.quote+"."+field
      case TupleDictProj(dict) => dict.quote+".tupleDict"
      case DictCUnion(d1, d2) => s"(${d1.quote}) DictUnion (${d2.quote})"
      case BindTupleDict(x, d) => x.name+" := "+d.quote
      case BindBagDict(x, d) => x.name+" := "+d.quote
      case CLookup(l, d) => s"Lookup(${l.quote}, ${d.quote})"
      case _ => self+"" //throw new IllegalArgumentException("unknown type")
    }

    // todo override equals
    def equalsVar(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].equalsVar(v)
      case t:BagType => self.asInstanceOf[BagCalc].equalsVar(v)
      case t:TupleType => self.asInstanceOf[TupleCalc].equalsVar(v)
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].equalsVar(v)
      case t:LabelType => self.asInstanceOf[LabelType].equals(v)
      case t:DictType => self.asInstanceOf[DictType].equals(v)
      case _ => false
    }
    
    def checkLabel(v: VarDef): Boolean = self match {
      case v:Var => self.equalsVar(v.varDef)
      case CLabel(i, vars) => vars.map(v2 => v2._2.equalsVar(v)).toList.contains(true)
      case _ => false
    }

    def pred1(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].pred1(v)
      case _ => false
    }
 
    def pred2(v: VarDef, w: List[VarDef]): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].pred2(v, w)
      case _ => false
    }
   
    def normalize: CompCalc = self.tp match {
      case t:BagType => self match {
        case CNamed(v, e) => CNamed(v, e.normalize)
        case _ => self.asInstanceOf[BagCalc].normalize
      }
      case t:TupleType => self match {
        case CSequence(exprs) => CSequence(exprs.map(_.normalize))
        case _ => self.asInstanceOf[TupleCalc].normalize
      }
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
      case t:LabelType => self.asInstanceOf[LabelCalc].normalize
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].normalize
      case t:DictType => self.asInstanceOf[DictCalc].normalize
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case t:BagType => self match {
        case CNamed(v2, e) => CNamed(v2, e.bind(e2, v))
        case _ => self.asInstanceOf[BagCalc].bind(e2, v)
      }
      case t:TupleType => self match {
        case CSequence(exprs) => CSequence(exprs.map(_.bind(e2, v)))
        case _ => self.asInstanceOf[TupleCalc].bind(e2, v)
      }
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].bind(e2, v)
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].bind(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].bind(e2, v)
      case t:DictType => self.asInstanceOf[DictCalc].bind(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
    }

    def substitute(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case y if self == e2 => Var(v)
      case t:BagType => self match {
        case CNamed(v2, e) => CNamed(v2, e.substitute(e2, v)) 
        case _ => self.asInstanceOf[BagCalc].substitute(e2, v)
      }
      case t:TupleType => self match {
        case CSequence(exprs) => CSequence(exprs.map(_.substitute(e2, v)))
        case _ => self.asInstanceOf[TupleCalc].substitute(e2, v)
      }
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].substitute(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].substitute(e2, v)
      case t:DictType => self.asInstanceOf[DictCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
    }

  }

  implicit class TupleAttributeCalcOps(self: TupleAttributeCalc) {
    
    def equalsVar(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].equalsVar(v)
      case t:BagType => self.asInstanceOf[BagCalc].equalsVar(v)
      case t:LabelType => self.asInstanceOf[LabelCalc].equalsVar(v)
      case _ => false 
    }

    def pred1(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].pred1(v)
      case _ => false
    }

    def pred2(v: VarDef, w: List[VarDef]): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].pred2(v, w)
      case _ => false
    }

    def normalize: CompCalc = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc].normalize
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
      case t:LabelType => self.asInstanceOf[LabelCalc].normalize
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc].bind(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].bind(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].bind(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot bind ${self}")
    }

    def substitute(e2: CompCalc, v: VarDef): TupleAttributeCalc = self.tp match {
      case y if self == e2 => Var(v).asInstanceOf[TupleAttributeCalc]
      case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
    }

  }


  implicit class PrimitiveCalcOps(self: PrimitiveCalc) {
    
    def equalsVar(v: VarDef): Boolean = self match {
      case ProjToPrimitive(vd:TupleVar, field) => vd.varDef == v
      case PrimitiveVar(vd) => vd == v
      case _ => false
    }

    def pred1(v: VarDef): Boolean = self match {
      case AndCondition(e1, e2) => e1.pred1(v) && e2.pred1(v)
      case OrCondition(e1, e2) => e1.pred1(v) && e2.pred1(v)
      case NotCondition(e1) => e1.pred1(v)
      case Conditional(op, e1, e2:Constant) => e1.equalsVar(v)
      case Conditional(op, e1:Constant, e2) => e2.equalsVar(v)
      case Conditional(op, e1, e2) => (e1 == e2) && (e1.equalsVar(v) || e2.equalsVar(v))
      case _ => false
    }

    
    def pred2(v: VarDef, w: List[VarDef]): Boolean = self match {
      case NotCondition(e1) => e1.pred2(v, w)
      case OrCondition(e1, e2) => e1.pred2(v, w) && e2.pred2(v, w)
      case AndCondition(e1, e2) => e1.pred2(v, w) && e2.pred2(v, w)
      case Conditional(op, e1, e2) => op match {
        case OpEq => // equals is a join condition
          (e1.equalsVar(v)  && w.filter{ v1 => e2.checkLabel(v1) }.nonEmpty) ||
            (e2.equalsVar(v) && w.filter{ v1 => e1.checkLabel(v1) }.nonEmpty)
        case _ => // any other operation is a filter later (pushed to the nest)  
          (e1.equalsVar(v) && w.filter{ v1 => e2.checkLabel(v1) }.nonEmpty) ||
            (e2.equalsVar(v) && w.filter{ v1 => e1.checkLabel(v1) }.nonEmpty)
        }
      case _ => false
    }

    def normalize: CompCalc = self match {
      case ProjToPrimitive(t @ Tup(fs), f) => fs.get(f).get 
      case Conditional(o, e1, e2) => Conditional(o, e1.normalize, e2.normalize)
      case NotCondition(e1) => NotCondition(e1.normalize)
      case AndCondition(e1, e2) => AndCondition(e1.normalize, e2.normalize)
      case OrCondition(e1, e2) => OrCondition(e1.normalize, e2.normalize)
      case CountComp(e, qs) => 
        CountComp(e.normalize.asInstanceOf[PrimitiveCalc], qs.map(_.normalize))
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
      case ProjToPrimitive(t, fs) => Proj(t.bind(e2, v).asInstanceOf[TupleCalc], fs)
      case y if self.equalsVar(v) => e2
      case BindPrimitive(x, e1) => BindPrimitive(x, e1.bind(e2, v).asInstanceOf[PrimitiveCalc])
      case Conditional(o, e1, e3) => Conditional(o, e1.bind(e2, v), e3.bind(e2, v))
      case NotCondition(e1) => NotCondition(e1.bind(e2, v))
      case AndCondition(e1, e3) => AndCondition(e1.bind(e2, v), e3.bind(e2, v))
      case OrCondition(e1, e3) => OrCondition(e1.bind(e2, v), e3.bind(e2, v))
      case CountComp(e, s) => 
        CountComp(e.bind(e2, v).asInstanceOf[PrimitiveCalc], s.map(_.bind(e2,v)))
      case _ => self
    } 

    def substitute(e2: CompCalc, v: VarDef): PrimitiveCalc = self match {
      case y if (self == e2) => Var(v).asInstanceOf[PrimitiveCalc]
      case Conditional(o, e1, e3) => Conditional(o, e1.substitute(e2, v), e3.substitute(e2, v))
      case NotCondition(e1) => NotCondition(e1.substitute(e2, v))
      case AndCondition(e1, e3) => AndCondition(e1.substitute(e2, v), e3.substitute(e2, v))
      case OrCondition(e1, e3) => OrCondition(e1.substitute(e2, v), e3.substitute(e2, v))
      case BindPrimitive(x,y) => BindPrimitive(x, y.substitute(e2, v))
      case CountComp(e, s) => CountComp(e.substitute(e2, v), s.map(_.bind(e2, v)))
      case _ => self
    }

  }

  implicit class BagCalcOps(self: BagCalc) {
              
    def equalsVar(v: VarDef): Boolean = self match {
      case ProjToBag(vd:TupleVar, field) => vd.varDef == v
      case BagVar(vd) => vd == v
      case CLookup(l, d @ BagDictVar(vd)) => vd == v 
      case _ => false
    }

    def normalize: CompCalc = self match {
      case ProjToBag(t @ Tup(fs), f) => fs.get(f).get
      case Generator(x, b) => b.normalize match {
        case Sng(e) => Bind(x, e)
        case e => Bind(x, e)
      }
      case BagComp(e, Nil) => Sng(e).normalize
      case BagComp(e, quals) =>
        val b = BagComp(e.normalize.asInstanceOf[TupleCalc], 
                  quals.map(_.normalize).filter(_ != Constant(true, BoolType)))
        if (b.qs.filter(_.isEmptyGenerator).nonEmpty) Zero()
        else if (b.hasIfGenerator){
          val ifs = b.qs.filter(_.isIfGenerator)
          ifs.head match {
            case Generator(x, IfStmt(ps, e1, e2 @ Some(a))) => // N4
              val qsn = b.qs.filterNot(Set(ifs.head))
              Merge(BagComp(b.e, List(ps, Generator(x, e1)) ++ qsn),
                  BagComp(b.e, List(NotCondition(ps), Generator(x, a)) ++ qsn)).normalize
            case _ => self
          }
        }
        else if (b.hasMergeGenerator){
          val mg = b.qs.filter(_.isMergeGenerator)
          mg.head match {
            case Generator(x, Merge(e1, e2)) => // N7
              val qsn = b.qs.filterNot(Set(mg.head))
              Merge(BagComp(b.e, Generator(x, e1) +: qsn), BagComp(b.e, Generator(x, e2) +: qsn)).normalize
            case _ => self
          }
        }
        else if (b.hasBagCompGenerator){
          val bg = b.qs.filter(_.isBagCompGenerator)
          bg.head match { // N8
            case Generator(x, BagComp(e1, qs1)) =>
              val lqsn = b.qs.slice(0, b.qs.indexOf(bg.head))
              val rqsn = b.qs.slice(b.qs.indexOf(bg.head)+1, b.qs.size)
              BagComp(b.e.asInstanceOf[TupleCalc], ((lqsn ++ qs1) ++ rqsn)).bind(e1, x).normalize
            case _ => self
          }
        }
        else if (b.hasBind){
          val getbind = b.qs.filter(_.isBind)
          val qsn = b.qs.filterNot(Set(getbind.head))
          getbind match {
            case Nil => self
            case _ => 
              val head = getbind.head.asInstanceOf[Bind]
              BagComp(b.e.asInstanceOf[TupleCalc], qsn).bind(head.e, head.x).normalize
          }
        }
        else b
      case s @ Sng(t @ Tup(e)) => 
        if (e.isEmpty) Zero()
        else Sng(t.normalize.asInstanceOf[TupleCalc])
      case Merge(e1, e2) => 
        Merge(e1.normalize.asInstanceOf[BagCalc], e2.normalize.asInstanceOf[BagCalc])
      case IfStmt(c @ Constant(true, BoolType), e1, e2) => e1
      case IfStmt(c @ Constant(false, BoolType), e1, e2) => e2 match {
        case Some(a) => a
        case _ => Zero()
      }
      case IfStmt(e1, e2, e3 @ Some(a)) => 
        IfStmt(e1.normalize.asInstanceOf[PrimitiveCalc], 
                e2.normalize.asInstanceOf[BagCalc], Some(a.normalize.asInstanceOf[BagCalc]))
      case IfStmt(e1, e2, e3 @ None) =>
        IfStmt(e1.normalize.asInstanceOf[PrimitiveCalc], e2.normalize.asInstanceOf[BagCalc], None)
      case CLookup(l, d) => CLookup(l.normalize.asInstanceOf[LabelCalc], d.normalize.asInstanceOf[BagDictCalc])
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
      case ProjToBag(t, fs) => Proj(t.bind(e2, v).asInstanceOf[TupleCalc], fs)
      case y if self.equalsVar(v) => e2
      case IfStmt(cond, e3, e4 @ Some(a)) => 
        IfStmt(cond.bind(e2, v).asInstanceOf[PrimitiveCalc], 
          e3.bind(e2, v).asInstanceOf[BagCalc], Option(a.bind(e2, v).asInstanceOf[BagCalc]))
      case IfStmt(cond, e3, e4 @ None) => 
        IfStmt(cond.bind(e2, v).asInstanceOf[PrimitiveCalc], e3.bind(e2, v).asInstanceOf[BagCalc], None)
      case Generator(x, y) => Generator(x, y.bind(e2, v).asInstanceOf[BagCalc])
      case Sng(e1) => Sng(e1.bind(e2, v).asInstanceOf[TupleCalc])
      case Merge(e1, e3) => Merge(e1.bind(e2, v).asInstanceOf[BagCalc], e3.bind(e2, v).asInstanceOf[BagCalc])
      case BagComp(e1, qs) => BagComp(e1.bind(e2, v).asInstanceOf[TupleCalc], qs.map(_.bind(e2, v)))
      case CLookup(l, d) => CLookup(l.bind(e2, v).asInstanceOf[LabelCalc], d.bind(e2, v).asInstanceOf[BagDictCalc])
      case _ => self
    }

    def substitute(e2: CompCalc, v: VarDef): BagCalc = self match {
      case ProjToBag(t, fs) => 
        Proj(t.substitute(e2, v).asInstanceOf[TupleCalc], fs).asInstanceOf[BagCalc]
      case y if self == e2 => Var(v).asInstanceOf[BagCalc]
      case IfStmt(cond, e3, e4 @ Some(a)) => 
        IfStmt(cond.substitute(e2, v), e3.substitute(e2, v), Option(a.substitute(e2, v)))
      case IfStmt(cond, e3, e4 @ None) => 
        IfStmt(cond.substitute(e2, v), e3.substitute(e2, v), None)
      case Generator(x, y) => Generator(x, y.substitute(e2, v))
      case Sng(e1) => Sng(e1.substitute(e2, v))
      case Merge(e1, e3) => Merge(e1.substitute(e2, v), e3.substitute(e2, v))
      case BagComp(e1, qs) => BagComp(e1.substitute(e2, v), qs.map(_.substitute(e2, v)))
      case CLookup(l, d) => 
        CLookup(l.substitute(e2, v).asInstanceOf[LabelCalc], d.substitute(e2, v).asInstanceOf[BagDictCalc])
      case _ => self
    }

  }

  implicit class TupleCalcOps(self: TupleCalc) {
    
    def equalsVar(v: VarDef): Boolean = self match {
      case TupleVar(vd) => vd == v
      case _ => false
    }

    def normalize: CompCalc = self match {
      case Tup(fs) => Tup(fs.map(f => f._1 -> f._2.normalize.asInstanceOf[TupleAttributeCalc]))
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
      case y if self.equalsVar(v) => e2
      case BindTuple(x, e1) => BindTuple(x, e1.bind(e2, v).asInstanceOf[TupleCalc])
      case Tup(fs) => Tup(fs.map(f => f._1 -> f._2.bind(e2, v).asInstanceOf[TupleAttributeCalc]))
      case _ => self
    }

    def substitute(e2: CompCalc, v: VarDef): TupleCalc = self match {
      case y if self == e2 => Var(v).asInstanceOf[TupleCalc]
      case BindTuple(x,y) => BindTuple(x, y.substitute(e2, v))
      case Tup(fs) => Tup(fs.map(f => f._1 -> f._2.substitute(e2, v)))
      case _ => self
    }

   }

   implicit class LabelCalcOps(self: LabelCalc){
      
      def equalsVar(v: VarDef): Boolean = self match {
        case LabelVar(vd) => vd == v
        case _ => false
      }

      def normalize: CompCalc = self match {
        case LabelProj(t @ Tup(fs), f) => fs.get(f).get
        case CLabel(id, vars) => CLabel(id, vars.map(v => v._1 -> v._2.normalize))
        case _ => self
      }

      def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
        case y if self.equalsVar(v) => e2
        case BindLabel(x,e) => BindLabel(x, e.bind(e2, v).asInstanceOf[LabelCalc])
        case LabelProj(t, f) => LabelProj(t.bind(e2, v).asInstanceOf[TupleCalc], f)
        case CLabel(id, vars) => CLabel(id, vars.map(v2 => v2._1 -> v2._2.bind(e2, v)))
        case _ => self
      }

      def substitute(e2: CompCalc, v: VarDef): LabelCalc = self match {
        case y if self == e2 => LabelVar(v)
        case BindLabel(x,e) => BindLabel(x, e.substitute(e2, v).asInstanceOf[LabelCalc])
        case LabelProj(t, f) => LabelProj(t.substitute(e2, v).asInstanceOf[TupleCalc], f)
        case CLabel(id, vars) => CLabel(id, vars.map(v2 => v2._1 -> v2._2.substitute(e2, v)))
        case _ => self
      }

    }

  implicit class DictCalcOps(self: DictCalc){
    
    def equalsVar(v: VarDef): Boolean = self match {
      case d:DictVar => d.varDef == v
      case _ => false
    }

    // these are also optimizations in shredding
    def normalize: CompCalc = self match {
      case BagCDict(l, f, d) => 
        BagCDict(l.normalize.asInstanceOf[LabelCalc], 
          f.normalize.asInstanceOf[BagCalc], d.normalize.asInstanceOf[TupleDictCalc])
      case TupleCDict(fs) => 
        TupleCDict(fs.map(f => f._1 -> f._2.normalize.asInstanceOf[TupleDictAttributeCalc]))
      case BagDictProj(dict @ TupleCDict(fs), field) => fs.get(field).get.normalize
      case TupleDictProj(dict @ BagCDict(l, f, d)) => d.normalize
      case BagDictProj(dict @ TupleDictComp(td, b @ BindTupleDict(x,e)), field) =>
        BagDictProj(td.bind(e, x).normalize.asInstanceOf[TupleDictCalc], field).normalize
      case TupleDictComp(td, b @ BindTupleDict(x, e)) => td.bind(e, x).normalize 
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
      case y if self.equalsVar(v) => e2
      case BagCDict(l, f, d) => 
        BagCDict(l.bind(e2, v).asInstanceOf[LabelCalc], 
          f.bind(e2, v).asInstanceOf[BagCalc], d.bind(e2, v).asInstanceOf[TupleDictCalc])
      case TupleCDict(fs) => 
        TupleCDict(fs.map(f => f._1 -> f._2.bind(e2, v).asInstanceOf[TupleDictAttributeCalc]))
      case BindBagDict(x, d) => BindDict(x, d.bind(e2, v).asInstanceOf[DictCalc])
      case BindTupleDict(x, d) => BindDict(x, d.bind(e2, v).asInstanceOf[DictCalc])
      case TupleDictComp(td, b) => 
        TupleDictComp(td.bind(e2, v).asInstanceOf[TupleDictCalc], b.bind(e2, v).asInstanceOf[BindTupleDict])
      case BagDictProj(dict, field) => BagDictProj(dict.bind(e2, v).asInstanceOf[TupleDictCalc], field)
      case TupleDictProj(dict) => TupleDictProj(dict.bind(e2, v).asInstanceOf[BagDictCalc])
      case _ => self
    }

    def substitute(e2: CompCalc, v: VarDef): DictCalc = self match {
      case y if self == e2 => DictVar(v)
      case BagCDict(l, f, d) => 
        BagCDict(l.substitute(e2, v), f.substitute(e2, v), d.substitute(e2, v).asInstanceOf[TupleDictCalc])
      case TupleCDict(fs) => 
        TupleCDict(fs.map(f => f._1 -> f._2.substitute(e2, v).asInstanceOf[TupleDictAttributeCalc]))
      case BindBagDict(x, d) => BindDict(x, d.substitute(e2, v).asInstanceOf[CompCalc]).asInstanceOf[DictCalc]
      case BindTupleDict(x, d) => BindDict(x, d.substitute(e2, v).asInstanceOf[CompCalc]).asInstanceOf[DictCalc]
      case TupleDictComp(td, b) => 
        TupleDictComp(td.substitute(e2, v).asInstanceOf[TupleDictCalc], 
          b.substitute(e2, v).asInstanceOf[BindTupleDict])
      case BagDictProj(dict, field) => BagDictProj(dict.substitute(e2, v).asInstanceOf[TupleDictCalc], field)
      case TupleDictProj(dict) => TupleDictProj(dict.substitute(e2, v).asInstanceOf[BagDictCalc])
      case _ => self
    }

  }

}
