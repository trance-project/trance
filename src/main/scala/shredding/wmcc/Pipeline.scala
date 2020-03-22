package shredding.wmcc

import shredding.core._
import shredding.nrc._

trait PipelineRunner extends MaterializeNRC
  with Materialization
  with Shredding
  with Printer
  with Optimizer 
  with NRCTranslator {

  def shredPipelineNew(program: Program, domains: Boolean = false): MaterializedProgram = {
    println("\nProgram:\n")
    println(quote(program))
    val shredProgram = shred(program)
//    println("\nShredded:\n")
//    println(quote(shredProgram))
    val optShredProgram = optimize(shredProgram)
//    println("\nOptimized:\n")
//    println(quote(optShredProgram))
    val matProgram =
      if (domains) materialize(optShredProgram)
      else sys.error("Not implemented yet")
    // TODO: Domain elimination
    println("\nMaterialized:\n")
    println(quote(matProgram.program))
    matProgram
  }

  def shredPipeline(query: Expr): CExpr = {
      val program = Program("Q", query)
      println("\nProgram:\n")
      println(quote(program))
      val sq = shred(program)
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

    val tr1 = TupleVarRef("r1", r1type)
    val tr2 = TupleVarRef("r2", r2type)

    val ttype = BagType(r1type)
    val br = BagVarRef("R", ttype)
    
    val rflat = NewLabel()
    val bagdict = 
    BagDict(
      rflat.tp,
      ForeachUnion(tr1, br,
        Singleton(Tuple("h" -> tr1("h"), "j" -> 
          NewLabel(ProjectLabelParameter(PrimitiveProject(tr1, "index")))))),
      TupleDict(Map("h" -> EmptyDict, "j" -> 
        // now we have to repeat the forloop above, 
        // but it might be possible to change the language a bit so 
        // we do not have to do this
        // this extra iteration is what linearization (ie. materialization helps us avoid)
        BagDict(
          rflat.tp, // just another dummy label (i think ???)
          ForeachUnion(tr1, br,
            ForeachUnion(tr2, tr1("j").asInstanceOf[BagExpr],
              Singleton(Tuple("m" -> tr2("m"), "n" -> tr2("n"), "k" -> 
                NewLabel(ProjectLabelParameter(PrimitiveProject(tr2, "index"))))))),
          TupleDict(Map("m" -> EmptyDict, "n" -> EmptyDict, "k" -> EmptyDict))
          // above "k" -> EmptyDict should be another bag dict expr, this is another TODO like in the last document
        )  
      ))          
    )
    quote(bagdict) 
  }

}
