package framework.examples

import framework.common._


// res: {sample: String, g_gene_name: String, burden: Double}
object flatten1 extends GenomicSchema {
  //gtfs( "g_contig" -> StringType, "g_start" -> IntType,"g_end" -> IntType,"gene_name" -> StringType)

  /*
    val variantType = TupleType(
      "contig" -> StringType,
      "start" -> IntType,
      "reference" -> StringType,
      "alternate" -> StringType,
      "genotypes" -> BagType(genoType))

    val genoType = TupleType("g_sample" -> StringType, "call" -> DoubleType)

  */

  val name = "variants_gene"
  val query =
    ForeachUnion(vr, variants,
      ForeachUnion(gtfr, gtfs,
        IfThenElse(
          Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")) && Cmp(OpGe, gtfr("g_end"), vr("start")),
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            Singleton(Tuple("sample" -> gr("g_sample"), "g_gene_name" -> gtfr("gene_name"),
              "burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))))
          )
        )
      )
    )
  val program = Program(Assignment(name, query))
}

// res: {pathway_name: String, p_gene_name: String}
object pathway_flatten extends GenomicSchema {
  val name = "pathway_flatten"
  val query =
    ForeachUnion(pr, pathways,
      ForeachUnion(ger, BagProject(pr, "gene_set"), // for ger in p.gene_set union
        Singleton(Tuple("pathway_name" -> pr("p_name"), "p_gene_name" -> ger("name")))
      )
    )
  val program = Program(Assignment(name, query))
}

// res: {sample, {pathway_name, burden}}
object plan4 extends GenomicSchema {
  val name = "pathway_burden_plan4"
  val (flatten, g) = varset(flatten1.name, "v2", flatten1.program(flatten1.name).varRef.asInstanceOf[BagExpr])
  val (pathwayFlat, p) = varset(pathway_flatten.name, "v3", pathway_flatten.program(pathway_flatten.name).varRef.asInstanceOf[BagExpr])
  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(g, flatten,
              IfThenElse(Cmp(OpEq, mr("m_sample"), g("sample")),

                ForeachUnion(p, pathwayFlat,
                  IfThenElse(Cmp(OpEq, g("g_gene_name"), p("p_gene_name")),
                    Singleton(Tuple("pathway_name" -> p("pathway_name"), "burden" -> g("burden")))
                  )))
            ),
            List("pathway_name"),
            List("burden"))
      ))
    )

  val p1 = Assignment(pathway_flatten.name, pathway_flatten.query.asInstanceOf[Expr])
  val p2 = Assignment(flatten1.name, flatten1.query.asInstanceOf[Expr])

  val program = Program(p2, p1, Assignment(name, query))

}


// res: {sample: String, name: String, burden: Double}
object gene_burden extends GenomicSchema {
  val name = "Gene_Burden"
  val query =
    ReduceByKey(
      ForeachUnion(vr, variants,
        ForeachUnion(gtfr, gtfs,
          IfThenElse(
            Cmp(OpGe, vr("start"), gtfr("g_start")) &&
              Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
            ForeachUnion(gr, BagProject(vr, "genotypes"),

              Singleton(Tuple("sample" -> gr("g_sample"), "name" -> gtfr("gene_name"),
                "burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
              ))
            )
          )
        )
      ),
      List("sample", "name"),
      List("burden")
    )
  val program = Program(Assignment(name, query))
}

// res: {sample, pathways: {pathway_name, burden}}
object plan5 extends GenomicSchema {
  val name = "pathway_burden_plan5"

  val (flatten, g) = varset(gene_burden.name, "v2", gene_burden.program(gene_burden.name).varRef.asInstanceOf[BagExpr])
  val (pathwayFlat, p) = varset(pathway_flatten.name, "v3", pathway_flatten.program(pathway_flatten.name).varRef.asInstanceOf[BagExpr])


  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(g, flatten,
              IfThenElse(Cmp(OpEq, mr("m_sample"), g("sample")),

                ForeachUnion(p, pathwayFlat,
                  IfThenElse(Cmp(OpEq, p("p_gene_name"), g("name")),
                    Singleton(Tuple("pathway_name" -> p("pathway_name"), "burden" -> g("burden")))
                  )))
            ),
            List("pathway_name"),
            List("burden"))
      ))
    )
  //  val p1 = pathway_flatten.asInstanceOf[plan5.Program].get(pathway_flatten.name).get
  //  val program = gene_burden.program.asInstanceOf[plan5.Program].append(p1).append(Assignment(name, query))


  val p1 = Assignment(pathway_flatten.name, pathway_flatten.query.asInstanceOf[Expr])
  val p2 = Assignment(gene_burden.name, gene_burden.query.asInstanceOf[Expr])
  val program = Program(p2, p1, Assignment(name, query))

}


// for plan 6
// res: {g_gene_name: String, genotypes: {g_sample, g_call}}
object variantsMapped extends GenomicSchema {
  val name = "variants_gene_plan6"
  val query =
    ForeachUnion(vr, variants,
      ForeachUnion(gtfr, gtfs,
        IfThenElse(
          Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")) && Cmp(OpGe, gtfr("g_end"), vr("start")),
          Singleton(Tuple("g_gene_name" -> gtfr("gene_name"), "g_genotypes" ->
            ForeachUnion(gr, BagProject(vr, "genotypes"),
              Singleton(Tuple("g_sample" -> gr("g_sample"), "g_call" -> gr("call"))
              )
            )
          ))
        )
      )
    )
  val program = Program(Assignment(name, query))
}


// res: {gb_sample: String, genes:(gb_name: String, gb_burden: Double)}
object Gene_Burden_plan6 extends GenomicSchema {
  val name = "Gene_Burden_plan6"

  val (vs, v) = varset(variantsMapped.name, "v5", variantsMapped.program(variantsMapped.name).varRef.asInstanceOf[BagExpr])
  val gb = TupleVarRef("v6", v("g_genotypes").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "gb_sample" -> mr("m_sample"), "genes" ->
          ForeachUnion(v, vs,
            ForeachUnion(gb, BagProject(v, "g_genotypes"),
              IfThenElse(
                Cmp(OpEq, gb("g_sample"), mr("m_sample")),
                Singleton(Tuple(
                  "gb_name" -> v("g_gene_name"),
                  "gb_burden" -> PrimitiveIfThenElse(Cmp(OpNe, gb("g_call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
                ))
              )
            )
          )
      ))
    )
  val program = variantsMapped.program.asInstanceOf[Gene_Burden_plan6.Program].append(Assignment(name, query))
}


// {sample, {pathway_name, burden}}
object plan6 extends GenomicSchema {
  val name = "pathway_burden_plan6"

  val (ss, s) = varset(Gene_Burden_plan6.name, "v4", Gene_Burden_plan6.program(Gene_Burden_plan6.name).varRef.asInstanceOf[BagExpr])
  val gs = TupleVarRef("g3", s("genes").asInstanceOf[BagExpr].tp.tp)

  // flatpathway: {pathway_name: String, p_gene_name: String}
  val (ps, p) = varset(pathway_flatten.name, "p1", pathway_flatten.program(pathway_flatten.name).varRef.asInstanceOf[BagExpr])

  val query =
    ForeachUnion(s, ss,

      Singleton(Tuple(
        "sample" -> s("gb_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(gs, s("genes").asBag,
              ForeachUnion(gs, s("genes").asBag,
                ForeachUnion(p, ps,
                  IfThenElse(Cmp(OpEq, gs("gb_name"), p("p_gene_name")),
                    Singleton(Tuple("pathway_name" -> p("pathway_name"), "burden" -> gs("gb_burden")))
                  )
                ))
            ),
            List("pathway_name"),
            List("burden")))
      )
    )



  val p0 = Assignment(variantsMapped.name, variantsMapped.query.asInstanceOf[Expr])
  val p1 = Assignment(pathway_flatten.name, pathway_flatten.query.asInstanceOf[Expr])
  val p2 = Assignment(Gene_Burden_plan6.name, Gene_Burden_plan6.query.asInstanceOf[Expr])
  val program = Program(p0, p1, p2, Assignment(name, query))
}



object flattenBurden extends GenomicSchema {
  val name = "falttenBurden"

  val query =
    ReduceByKey(
      ForeachUnion(vr, variants,
        ForeachUnion(gtfr, gtfs,
          IfThenElse(
            Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),

            ForeachUnion(gr, vr("genotypes").asBag,
              Singleton(Tuple("fb_sample" -> gr("g_sample"), "fb_gene" -> gtfr("gene_name"), "fb_burden" -> Const(1.0, DoubleType)))
            )


          )
        )
      ),
      List("fb_sample", "fb_gene"),
      List("fb_burden")
    )
  val program = Program(Assignment(name, query))
}


object geneBurdenAlt extends GenomicSchema {
  val name = "geneBurdenAlt"
  val (fbs, fb) = varset(flattenBurden.name, "v6", flattenBurden.program(flattenBurden.name).varRef.asInstanceOf[BagExpr])

  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple("sample" -> mr("m_sample"), "genes" ->
        ForeachUnion(fb, fbs,
          IfThenElse(Cmp(OpEq, fb("fb_sample"), mr("m_sample")),
            Singleton(Tuple("gene" -> fb("fb_gene"), "burden" -> fb("fb_burden")))
          )
        )
      ))
    )
  val program = flattenBurden.program.asInstanceOf[geneBurdenAlt.Program].append(Assignment(name, query))

}



object byGene extends GenomicSchema {
  val name = "byGene"
  val query =
    ForeachUnion(gtfr, gtfs,
      Singleton(Tuple("b_gene" -> gtfr("gene_name"), "samples" ->
        ReduceByKey(
          ForeachUnion(vr, variants,
            IfThenElse(Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
              ForeachUnion(gr, vr("genotypes").asBag,
                Singleton(Tuple("b_sample" -> gr("g_sample"),
                  "b_burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
                ))
              )
            )
          ),
          List("b_sample"),
          List("b_burden")
        ))
      )
    )
  val program = Program(Assignment(name, query))
}


object geneBurdenAlt2 extends GenomicSchema {
  val name = "geneBurdenAlt2"
  val (bgs, g) = varset(byGene.name, "v6", byGene.program(byGene.name).varRef.asInstanceOf[BagExpr])
  val b = TupleVarRef("g3", g("samples").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "samples" ->
          ForeachUnion(g, bgs,
            ForeachUnion(b, g("samples").asBag,
              IfThenElse(Cmp(OpEq, mr("m_sample"), b("b_sample")),

                Singleton(Tuple("gene" -> g("b_gene"), "burden" -> b("b_burden")))
              )
            )
          )
      ))
    )
  val program = byGene.program.asInstanceOf[geneBurdenAlt2.Program].append(Assignment(name, query))

}

