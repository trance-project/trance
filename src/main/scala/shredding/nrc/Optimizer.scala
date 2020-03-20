package shredding.nrc

/**
  * Simple optimizations:
  *
  *   - let inlining:
  *         let X = e1 in X => e1
  *
  *   - dead code elimination
  *         let X = e1 in e2 => e2 if not using X
  *
  */
trait Optimizer extends Extensions with Implicits with Factory {
  this: MaterializeNRC =>

  def optimize(p: ShredProgram): ShredProgram =
    ShredProgram(p.statements.map(optimize))

  def optimize(a: ShredAssignment): ShredAssignment =
    ShredAssignment(a.name, optimize(a.rhs))

  def optimize(e: ShredExpr): ShredExpr =
    ShredExpr(optimize(e.flat), optimize(e.dict).asInstanceOf[DictExpr])

  def optimize(p: Program): Program =
    Program(p.statements.map(optimize))

  def optimize(a: Assignment): Assignment =
    Assignment(a.name, optimize(a.rhs))

  def optimize(e: Expr): Expr = inline(deadCodeElimination(e))

  def inline(e: Expr): Expr = replace(e, {
    // Case: let X = e1 in X => e1
    case l: Let if (l.e2 match {
      case v2: VarRef if v2.varDef == l.x => true
      case _ => false
    }) => inline(l.e1)
  })

  def deadCodeElimination(e: Expr): Expr = replace(e, {
    // let X = e1 in e2 => e2 if not using X
    case l: Let if {
      val im = inputVars(l.e2).map(v => v.name -> v.tp).toMap
      // Sanity check
      im.get(l.x.name).foreach { tp =>
        assert(tp == l.x.tp,
          "[deadCodeElimination] Type differs " + tp + " and " + l.x.tp)
      }
      !im.contains(l.x.name)
    } => deadCodeElimination(l.e2)
  })

}
