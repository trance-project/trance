package shredding.calc

import shredding.core._
import shredding.nrc.Shredding

trait CalcImplicits {
  this: ShreddedCalc with NRCTranslator with Shredding =>

  implicit class CompCalcOps(self: CompCalc) {

    def toTupleAttributeCalc: TupleAttributeCalc = self.tp match {
      case t:BagType => self.asInstanceOf[TupleAttributeCalc]
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc]
      case t:LabelType => self.asInstanceOf[LabelCalc]
      case _ => sys.error("cannot cast type")
    }

    def toLabelAttributeCalc: LabelAttributeCalc = self.tp match {
      case t:PrimitiveCalc => self.asInstanceOf[LabelAttributeCalc]
      case t:TupleType => self.asInstanceOf[LabelAttributeCalc]
      case t:LabelType => self.asInstanceOf[LabelAttributeCalc]
      case _ => sys.error("cannot case type")
    }

    def toLabelCalc: LabelCalc = self.tp match {
      case t:LabelType => self.asInstanceOf[LabelCalc]
      case _ => sys.error("cannot cast type")
    }

    def toPrimitiveCalc: PrimitiveCalc = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc]
      case _ => sys.error("cannot cast type")
    }

    def toBagCalc: BagCalc = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc]
      case _ => sys.error("cannot cast type")
    }

    def toTupleCalc: TupleCalc = self.tp match {
      case t:TupleType => self.asInstanceOf[TupleCalc]
      case _ => sys.error("cannot cast type")
    }

    def checkLabel(v: VarDef): Boolean = self match {
      case p:Proj => p.tuple.checkLabel(v)
      case TupleVar(vd) => 
        if (v.tp.isLabel){
          v.tp.asInstanceOf[TupleType].attrs.get("lbl") match {
            case Some(label) => label.asInstanceOf[LabelType].attrs.contains(vd.name)
            case None => false
          }
        }else{ false }
      case _ => false
    }

    def equalsVar(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].equalsVar(v)
      case t:BagType => self.asInstanceOf[BagCalc].equalsVar(v)
      case t:TupleType => self.asInstanceOf[TupleCalc].equalsVar(v)
      case _ => false
    }

    def isOutputDict: Boolean = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc].isOutputDict
      case _ => false
    }

    def hasOutputDict: Boolean = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc].hasOutputDict
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
      case t:TupleType => self.asInstanceOf[TupleCalc].normalize
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].normalize
      case t:LabelAttributeType => self.asInstanceOf[LabelAttributeCalc].normalize
      case t:LabelType => self.asInstanceOf[LabelCalc].normalize
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case t:BagType => self.asInstanceOf[BagCalc].bind(e2, v)
      case t:TupleType => self.asInstanceOf[TupleCalc].bind(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].bind(e2, v)
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].bind(e2, v)
      case t:LabelAttributeType => self.asInstanceOf[LabelAttributeCalc].bind(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].bind(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
    }

    def substitute(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case y if self == e2 => Var(v)
      case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
      case t:TupleType => self.asInstanceOf[TupleCalc].substitute(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case t:TupleAttributeType => self.asInstanceOf[TupleAttributeCalc].substitute(e2, v)
      case t:LabelAttributeType => self.asInstanceOf[LabelAttributeCalc].substitute(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot substitute ${self}")
    }

  }

  implicit class TupleAttributeCalcOps(self: TupleAttributeCalc) {
    
    def equalsVar(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].equalsVar(v)
      case t:BagType => self.asInstanceOf[BagCalc].equalsVar(v)
      case t:TupleType => self.asInstanceOf[TupleCalc].equalsVar(v)
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
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
    }

    def substitute(e2: CompCalc, v: VarDef): TupleAttributeCalc = self.tp match {
      case y if self == e2 => Var(v).asInstanceOf[TupleAttributeCalc]
      case t:BagType => self.asInstanceOf[BagCalc].substitute(e2, v)
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
    }

  }

  implicit class LabelAttributeCalcOps(self: LabelAttributeCalc) {
    
    def equalsVar(v: VarDef): Boolean = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].equalsVar(v)
      case t:TupleType => self.asInstanceOf[TupleCalc].equalsVar(v)
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
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].normalize
      case t:TupleType => self.asInstanceOf[TupleCalc].normalize
      case t:LabelType => self.asInstanceOf[LabelCalc].normalize
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self.tp match {
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].bind(e2, v)
      case t:TupleType => self.asInstanceOf[TupleCalc].bind(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].bind(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
    }

    def substitute(e2: CompCalc, v: VarDef): LabelAttributeCalc = self.tp match {
      case y if self == e2 => Var(v).asInstanceOf[LabelAttributeCalc]
      case t:PrimitiveType => self.asInstanceOf[PrimitiveCalc].substitute(e2, v)
      case t:TupleType => self.asInstanceOf[TupleCalc].substitute(e2, v)
      case t:LabelType => self.asInstanceOf[LabelCalc].substitute(e2, v)
      case _ => throw new IllegalArgumentException(s"cannot normalize ${self}")
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
          (e1.equalsVar(v)  && w.filter{ 
              v1 => (e2.equalsVar(v1) || e2.checkLabel(v1)) }.nonEmpty) ||
            (e2.equalsVar(v) && w.filter{ 
                v1 => (e1.equalsVar(v1) || e1.checkLabel(v1)) }.nonEmpty)
        case _ => // any other operation is a filter later (pushed to the nest)  
          (e1.equalsVar(v) && w.filter{ 
              v1 => e2.equalsVar(v1) || e2.checkLabel(v1) }.nonEmpty) ||
            (e2.equalsVar(v) && w.filter{ 
                v1 => e1.equalsVar(v1) || e1.checkLabel(v1) }.nonEmpty)
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
       
    def isOutputDict: Boolean = self match {
      case Generator(x, v @ CLookup(lbl, dict)) => dict match {
        case t:OutputBagDict => true
        case _ => false
      }
      case _ => false
    }

    def hasOutputDict: Boolean = self match {
      case BagComp(e, qs) => qs.map(_.isOutputDict).contains(true)
      case _ => false
    }
       
    def equalsVar(v: VarDef): Boolean = self match {
      case ProjToBag(vd:TupleVar, field) => vd.varDef == v
      case BagVar(vd) => vd == v
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
        val b = BagComp(e.normalize.asInstanceOf[TupleCalc], quals.map(_.normalize))
        //println("normalization "+b)
        //println(b.hasOutputDict)
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
        /**else if (b.hasOutputDict){
          val bg = b.qs.filter(_.isOutputDict) 
          bg.head match {
            case Generator(x, l @ CLookup(lbl, dict)) =>
              println("found an output dict generator "+b)
              val lqsn = b.qs.slice(0, b.qs.indexOf(bg.head))
              val rqsn = b.qs.slice(b.qs.indexOf(bg.head)+1, b.qs.size)
              val nc = l.unshred
              BagComp(b.e.asInstanceOf[TupleCalc], (lqsn :+ Generator(x, nc)) ++ rqsn).normalize
            case _ => self
          }
        }**/
        else if (b.hasBind){
          val getbind = b.qs.filter(_.isBind)
          val qsn = b.qs.filterNot(Set(getbind.head))
          getbind.head match {
            case BindPrimitive(x, e1) => 
              BagComp(b.e.asInstanceOf[TupleCalc], qsn).bind(e1, x).normalize
            case BindTuple(x, e1) => 
              BagComp(b.e.asInstanceOf[TupleCalc], qsn).bind(e1, x).normalize
            case _ => self
          }
        }
        else b
      case s @ Sng(t @ Tup(e)) => 
        if (e.isEmpty) Zero()
        else Sng(t.normalize.asInstanceOf[TupleCalc])
      case Merge(e1, e2) => 
        Merge(e1.normalize.asInstanceOf[BagCalc], e2.normalize.asInstanceOf[BagCalc])
      case IfStmt(e1, e2, e3 @ Some(a)) => 
        IfStmt(e1.normalize.asInstanceOf[PrimitiveCalc], 
                e2.normalize.asInstanceOf[BagCalc], Some(a.normalize.asInstanceOf[BagCalc]))
      case IfStmt(e1, e2, e3 @ None) =>
        IfStmt(e1.normalize.asInstanceOf[PrimitiveCalc], e2.normalize.asInstanceOf[BagCalc], None)
      case CLookup(lbl, dict) => CLookup(lbl.normalize.asInstanceOf[LabelCalc], dict)
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
      case CLookup(lbl, dict) => CLookup(lbl.bind(e2, v).asInstanceOf[LabelCalc], dict)
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
      case CLookup(lbl, dict) => CLookup(lbl.substitute(e2, v), dict)
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

  implicit class LabelCalcOps(self: LabelCalc) {
    def equalsVar(v: VarDef): Boolean = self match {
      case LabelVar(vd) => vd == v
      case _ => false
    }
    
    def normalize: CompCalc = self match {
      case ProjToLabel(t @ Tup(fs), f) => fs.get(f).get
      case CLabel(vars, id) => CLabel(vars.map(f => f.normalize.asInstanceOf[Var]), id)
      case _ => self
    }

    def bind(e2: CompCalc, v: VarDef): CompCalc = self match {
      case ProjToLabel(t, fs) => Proj(t.bind(e2, v).asInstanceOf[TupleCalc], fs)
      case BindLabel(x,e1) => BindLabel(x, e1.bind(e2, v).asInstanceOf[LabelCalc])
      case CLabel(vars, id) => CLabel(vars.map(f => f.bind(e2, v).asInstanceOf[Var]), id)
      case _ => self
    }

    def substitute(e2: CompCalc, v: VarDef): LabelCalc = self match {
      case y if self == e2 => Var(v).asInstanceOf[LabelCalc]
      case BindLabel(x,y) => BindLabel(x,y.substitute(e2, v))
      case CLabel(vars, id) => CLabel(vars.map(f => f.substitute(e2, v).asInstanceOf[Var]), id)
      case _ => self
    }
  }


}
