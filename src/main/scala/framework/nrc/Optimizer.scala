package framework.nrc

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

  def defaultOpt: Expr => Expr =
    e => inline(deadCodeElimination(e))

  def optimize(p: ShredProgram): ShredProgram = optimize(p, defaultOpt)

  def optimize(p: ShredProgram, f: Expr => Expr): ShredProgram =
    ShredProgram(p.statements.map(s => optimize(s, f)))

  def optimize(a: ShredAssignment): ShredAssignment = optimize(a, defaultOpt)

  def optimize(a: ShredAssignment, f: Expr => Expr): ShredAssignment =
    ShredAssignment(a.name, optimize(a.rhs, f))

  def optimize(e: ShredExpr): ShredExpr = optimize(e, defaultOpt)

  def optimize(e: ShredExpr, f: Expr => Expr): ShredExpr =
    ShredExpr(optimize(e.flat, f), optimize(e.dict, f).asInstanceOf[DictExpr])

  def optimize(p: Program): Program = optimize(p, defaultOpt)

  def optimize(p: Program, f: Expr => Expr = defaultOpt): Program =
    Program(p.statements.map(optimize(_, f)))

  def optimize(a: Assignment): Assignment = optimize(a, defaultOpt)

  def optimize(a: Assignment, f: Expr => Expr): Assignment =
    Assignment(a.name, optimize(a.rhs, f))

  def optimize(e: Expr): Expr = optimize(e, defaultOpt)

  def optimize(e: Expr, f: Expr => Expr): Expr = f(e)

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

  def pushAggregate(e: Expr): Expr = e

}
