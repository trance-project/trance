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
        ForeachUnion(gr, BagProject(vr, "genotypes"),
          IfThenElse(
            Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")) && Cmp(OpGe, gtfr("g_end"), vr("start")),
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
  val (pathwayFlat, p) = varset(pathway_flatten.name, "v2", pathway_flatten.program(pathway_flatten.name).varRef.asInstanceOf[BagExpr])
  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(g, flatten,
              IfThenElse(Cmp(OpEq, mr("m_sample"), g("sample")),

                ForeachUnion(p, pathwayFlat,
                  IfThenElse(Cmp(OpEq, p("p_gene_name"), g("g_gene_name")),
                    Singleton(Tuple("pathway_name" -> p("pathway_name"), "burden" -> g("burden")))
                  )))
            ),
            List("pathway_name"),
            List("burden"))
      ))
    )
  val p1 = pathway_flatten.asInstanceOf[plan4.Program].get(pathway_flatten.name).get
  val program = flatten1.program.asInstanceOf[plan4.Program].append(p1).append(Assignment(name, query))

}


// res: {sample: String, name: String, burden: Double}
object gene_burden extends GenomicSchema {
  val name = "Gene_Burden"
  val query =
    ReduceByKey(
      ForeachUnion(vr, variants,
        ForeachUnion(gtfr, gtfs,
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            IfThenElse(
              Cmp(OpGe, vr("start"), gtfr("g_start")) &&
                Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
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
  val (pathwayFlat, p) = varset(pathway_flatten.name, "v2", pathway_flatten.program(pathway_flatten.name).varRef.asInstanceOf[BagExpr])


  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(g, flatten,
              IfThenElse(Cmp(OpEq, mr("m_sample"), g("sample")),

                ForeachUnion(p, pathwayFlat,
                  IfThenElse(Cmp(OpEq, p("p_gene_name"), g("g_gene_name")),
                    Singleton(Tuple("pathway_name" -> p("pathway_name"), "burden" -> g("burden")))
                  )))
            ),
            List("pathway_name"),
            List("burden"))
      ))
    )
  val p1 = pathway_flatten.asInstanceOf[plan5.Program].get(pathway_flatten.name).get
  val program = gene_burden.program.asInstanceOf[plan5.Program].append(p1).append(Assignment(name, query))
}


// for plan 6
// res: {g_gene_name: String, burden: Double}
object flatten_plan6 extends GenomicSchema {
  val name = "variants_gene_plan6"
  val query =
    ForeachUnion(vr, variants,
      ForeachUnion(gtfr, gtfs,
        ForeachUnion(gr, BagProject(vr, "genotypes"),
          IfThenElse(
            Cmp(OpGe, vr("start"), gtfr("g_start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")) && Cmp(OpGe, gtfr("g_end"), vr("start")),
            Singleton(Tuple("g_gene_name" -> gtfr("gene_name"), "genotypes" -> vr("genotypes")
            ))
          )
        )
      )
    )
  val program = Program(Assignment(name, query))
}

//{gene_name, genotypes{}}
object variantsMapped extends GenomicSchema {
  val name = "variantsMapped"
  val query =
    ForeachUnion(gtfr, gtfs,

      Singleton(Tuple("gene_name" -> gtfr("gene_name"), "genotypes" ->

        ForeachUnion(vr, variants,
          ForeachUnion(gr, BagProject(vr, "genotypes"),

            IfThenElse(Cmp(OpGe, vr("start"), gtfr("g_start")) &&
              Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
              vr("genotypes").asBag
            )
          )
        )
      ))
    )
  val program = Program(Assignment(name, query))
}


// res: {gb_sample: String, genes:(gb_name: String, gb_burden: Double)}
object Gene_Burden_plan6 extends GenomicSchema {
  val name = "Gene_Burden_plan6"

  val (vs, v) = varset(variantsMapped.name, "v2", variantsMapped.program(variantsMapped.name).varRef.asInstanceOf[BagExpr])
  val g = TupleVarRef("g2", v("genotypes").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "gb_sample" -> mr("m_sample"), "genes" ->
          ForeachUnion(v, vs,
            ForeachUnion(g, v("genotypes").asBag,
              IfThenElse(
                Cmp(OpEq, g("g_sample"), mr("m_sample")),
                Singleton(Tuple(
                  "gb_name" -> v("gene_name"), "gb_burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
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

  val (ss, s) = varset(Gene_Burden_plan6.name, "v3", Gene_Burden_plan6.program(Gene_Burden_plan6.name).varRef.asInstanceOf[BagExpr])
  val gs = TupleVarRef("g3", s("genes").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(s, ss,

      Singleton(Tuple(
        "sample" -> s("gb_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(pr, pathways,
              ForeachUnion(ger, BagProject(pr, "gene_set"),
                ForeachUnion(gs, s("genes").asBag,
                  IfThenElse(Cmp(OpEq, gs("gb_name"), ger("name")),
                    Singleton(Tuple("pathway_name" -> pr("p_name"), "burden" -> gs("gb_burden")))
                  )
                ))
            ),
            List("pathway_name"),
            List("burden")))
      )
    )


  val program = Gene_Burden_plan6.program.asInstanceOf[plan6.Program].append(Assignment(name, query))
}