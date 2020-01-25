package shredding.wmcc

import shredding.core._
import shredding.nrc._

trait PipelineRunner extends ShredNRC
  with Materializer
  with Shredding
  with Printer
  with Optimizer 
  with NRCTranslator {

  def shredPipelineNew(program: Program, domains: Boolean = false): MaterializedProgram =
    program.statements.map(shredPipelineNew(_, domains)).reduce(_ append _)

  def shredPipelineNew(a: Assignment, domains: Boolean): MaterializedProgram =
    shredPipelineNew(a.rhs, domains)

  def shredPipelineNew(query: Expr, domains: Boolean): MaterializedProgram = {
    println("\nQuery:\n")
    println(quote(query))
    //val nq = nestingRewrite(query)
    //println("\nRewrite:\n")
    //println(quote(nq))
    val sq = shred(query)
    //println("\nShredded:\n")
    //println(quote(sq))
    //println("\nOptimized:\n")
    val sqo = optimize(sq)
    //println(quote(sqo))
    val lsq = if (domains) materialize(sqo) else materializeNoDomains(sqo)
    println("\nMaterialized:\n")
    println(quote(lsq.program))
    lsq

  }

//  def shredPipelineNew(query: Expr, domains: Boolean = false): Expr = query match {
//    case Sequence(fs) => Sequence(fs.map{
//      case n: Named =>
//        val sp = shredPipelineNew(n.e, domains) match {
//          case Sequence(nes) => nes.head
//          case ne => ne
//        }
//        // explore why the types wouldn't be the same here in the first place
//        Named(VarDef(n.name, sp.tp), sp.asInstanceOf[BagExpr])
//      case e1 => shredPipelineNew(e1, domains)
//    })
//    case _ =>
//      println("\nQuery:\n")
//      println(quote(query))
//      //val nq = nestingRewrite(query)
//      //println("\nRewrite:\n")
//      //println(quote(nq))
//      val sq = shred(query)
//      //println("\nShredded:\n")
//      //println(quote(sq))
//      //println("\nOptimized:\n")
//      val sqo = optimize(sq)
//      //println(quote(sqo))
//      val lsq = if (domains) linearize(sqo) else linearizeNoDomains(sqo)
//      println("\nLinearized:\n")
//      println(quote(lsq))
//      lsq
//  }

  def shredPipeline(query: Expr): CExpr = {
      println("\nQuery:\n")
      println(quote(query))
      val sq = shred(query)
      println("\nShredded:\n")
      println(quote(sq))
      //println("\nOptimized:\n")
      val sqo = optimize(sq)
      //println(quote(sqo))
      val lsq = materialize(sqo)
      println("\nMaterialized:\n")
      println(quote(lsq.program))
      translate(lsq.program)
  }

  /**
    * Example for value shredding
    */
  def makeBag = {

    val r2type = TupleType("index" -> IntType, "m" -> IntType, "n" -> IntType, "k" ->
                    BagType(TupleType("n" -> IntType)))
    val r1type = TupleType("index" -> IntType, "h" -> IntType, "j" -> BagType(r2type))

    val r1 = VarDef("r1", r1type)
    val tr1 = TupleVarRef(r1)

    val r2 = VarDef("r2", r2type)
    val tr2 = TupleVarRef(r2)

    val ttype = BagType(r1type)
    val r = VarDef("R", ttype)
    
    val rflat = NewLabel()
    val bagdict = 
    BagDict(
      rflat,
      ForeachUnion(r1, BagVarRef(r), 
        Singleton(Tuple("h" -> tr1("h"), "j" -> 
          NewLabel(Set(ProjectLabelParameter(PrimitiveProject(tr1, "index"))))))),
      TupleDict(Map("h" -> EmptyDict, "j" -> 
        // now we have to repeat the forloop above, 
        // but it might be possible to change the language a bit so 
        // we do not have to do this
        // this extra iteration is what linearization (ie. materialization helps us avoid)
        BagDict(
          rflat, // just another dummy label (i think ???)
          ForeachUnion(r1, BagVarRef(r),
            ForeachUnion(r2, tr1("j").asInstanceOf[BagExpr],
              Singleton(Tuple("m" -> tr2("m"), "n" -> tr2("n"), "k" -> 
                NewLabel(Set(ProjectLabelParameter(PrimitiveProject(tr2, "index")))))))),
          TupleDict(Map("m" -> EmptyDict, "n" -> EmptyDict, "k" -> EmptyDict))
          // above "k" -> EmptyDict should be another bag dict expr, this is another TODO like in the last document
        )  
      ))          
    )
    quote(bagdict) 
  }

}
