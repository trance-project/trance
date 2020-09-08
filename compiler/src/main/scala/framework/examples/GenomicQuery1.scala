package framework.examples


import framework.common._

object GenomicQuery1 extends GenomicSchema {

  val name = "GenomicQuery1"

  // this query just iterates over the variant set
  val query =
    ForeachUnion(vr, variants, // For v in Variants
      Singleton(Tuple("contig" -> vr("contig"), //   {( contig := v.contig,
        "start" -> vr("start"), //      start := v.start,
        "reference" -> vr("reference"), //       reference := v.reference,
        "alternate" -> vr("alternate"), //       alternate := v.alternate,
        "genotypes" -> //       genotypes :=
          ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
            Singleton(Tuple("sample" -> gr("g_sample"), //          {(sample := g.sample,
              "call" -> gr("call"))) //            call := g.call )}
          )))) //    )}

  /*
  * for v in subVCF union		// subvcf has the schema as {sample, contig, pos}
{<sample:= v.sample,
    for g in GTF union 	// GTF(contig: String, start: Int, end: Int, gene_name: String)
      genes:= sumBy^{total}_{name}(
        if v.contig == g.contig && v.pos >= g.start && v.pos >= g.start then        // join
          {<name: g.gene_name, total: 1>})
>}
  */

  //  {(sample: String, genes: { (name: String, burden: Int) })}
  val query4_1 =
    ForeachUnion(vr, variants, // For v in Variants
      ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
        Singleton(Tuple("sample" -> gr("g_sample"), //          {(sample := g.sample,
          "genes" -> //              genes := sumby
            ReduceByKey(
              ForeachUnion(gtfr, gtfs,
                IfThenElse(
                  And(
                    And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"), gtfr("g_start"))),
                    And(Cmp(OpGe, gtfr("g_end"), vr("start")), Cmp(OpEq, gtfr("g_contig"), vr("contig")))),
                  Singleton(Tuple("name" -> gtfr("gene_name"), "burden" -> Const(1, IntType)))
                )
              ),
              List("name"),
              List("burden")
            )
        ))
      )
    )
  //    val program = Program(Assignment(name, query4_1))
  // 4.1 result:
  //    GenomicQuery1 := For v in Variants Union
  //    For g in v.genotypes Union
  //            {( sample := g.sample,
  //                    genes := ReduceByKey([name], [burden],
  //                For gtf in Gtfs Union
  //                If (g.call != 0 AND v.start >= gtf.start AND gtf.end >= v.start AND gtf.contig = v.contig) Then
  //                        {( name := gtf.gene_name,
  //                                burden := 1 )}
  //                ) )}


  // 4.2  {(sample: String, pathways: {(name: String, genes: { (name: String, burden: Int)})})}
  val query4_2 =
    ForeachUnion(vr, variants, // For v in Variants
      ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
        Singleton(Tuple("sample" -> gr("g_sample"), //          {(sample := g.sample, genes :=
          "pathway" ->
            ForeachUnion(pr, pathways, // for p in Pathways union
              Singleton(Tuple("name" -> pr("p_name"), // pathway := {( name := p.name,
                "genes" -> ReduceByKey( //              (sumby(name)(burden)
                  ForeachUnion(gtfr, gtfs, // for gtfr in gtfs union
                    ForeachUnion(ger, BagProject(pr, "gene_set"), // for ger in p.gene_set union
                      IfThenElse(
                        And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
                          And(
                            And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"), gtfr("g_start"))),
                            And(Cmp(OpGe, gtfr("g_end"), vr("start")), Cmp(OpEq, gtfr("g_contig"), vr("contig"))))),
                        Singleton(Tuple("name" -> gtfr("gene_name"), "burden" -> Const(1, IntType)))
                      )
                    )
                  ), List("name"), List("burden"))
              )
              ))
        )
        )
      )
    )

  // result:
  // For v in Variants Union
  //  For g in v.genotypes Union
  //    {( sample := g.sample,
  //       pathway := For p in Pathways Union
  //      {( name := p.name,
  //         genes := ReduceByKey([name], [burden],
  //        For gtf in Gtfs Union
  //          For ge in p.gene_set Union
  //            If (gtf.gene_name = ge.name AND g.call != 0 AND v.start >= gtf.start AND gtf.end >= v.start AND gtf.contig = v.contig) Then
  //              {( name := gtf.gene_name,
  //                 burden := 1 )}
  //      ) )} )}


  // 4.3 {(sample: String, pathways: {(name: String, total_burden: Int, genes: { (name: String, burden: Int) })})}
  //    val itemTp = TupleType("name" -> StringType, "burden" -> IntType)
  //    val query4_3 =
  //        ForeachUnion(vr, variants,                                    // For v in Variants
  //            ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For g in v.genotypes union
  //                Singleton(Tuple("sample" -> gr("sample"),       //          {(sample := g.sample, genes :=
  ////                    "pathway" ->
  //                        ForeachUnion(pr, pathways,                                  // for p in Pathways union
  ////                            Singleton(Tuple("name" -> pr("p_name"),       // pathway := {( name := p.name,
  //                                Let(gene_burden, ReduceByKey(                       //              (sumby(name)(burden)
  //                                    ForeachUnion(gtfr, gtfs,                                    // for gtfr in gtfs union
  //                                        ForeachUnion(ger, BagProject(pr, "gene_set"),               // for ger in p.gene_set union
  //                                            IfThenElse(
  //                                                And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
  //                                                    And(
  //                                                        And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
  //                                                        And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig"))))),
  //                                                Singleton(Tuple("name" -> gtfr("gene_name"), "burden" -> Const(1, IntType)))
  //                                            )
  //                                        )
  //                                    ), List("name"), List("burden")),
  //
  //                                    "Pathways" -> Singleton(Tuple("name" -> pr("p_name"),
  //                                        "total_burden" -> ReduceByKey(                       //              (sumby(name)(burden)
  //                                            ForeachUnion(gtfr, gtfs,                                    // for gtfr in gtfs union
  //                                                ForeachUnion(ger, BagProject(pr, "gene_set"),               // for ger in p.gene_set union
  //                                                    IfThenElse(
  //                                                        And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
  //                                                            And(
  //                                                                And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
  //                                                                And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig"))))),
  //                                                        Singleton(Tuple("name" -> gtfr("gene_name"), "burden" -> Const(1, IntType)))
  //                                                    )
  //                                                )
  //                                            ), List("name"), List("burden")),
  //                                        "gene" -> gene_burden
  //                                    ))
  //                                )
  //                            )
  //                        )
  //                )
  //            )
  //        )


  // 4.4
  val query4_4 =
    ForeachUnion(vr, variants, // For v in Variants
      ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
        Singleton(Tuple("sample" -> gr("g_sample"), //          {(sample := g.sample, genes :=
          "pathway" ->
            ForeachUnion(pr, pathways, // for p in Pathways union
              ReduceByKey( //              (sumby(name)(burden)
                ForeachUnion(gtfr, gtfs, // for gtfr in gtfs union
                  ForeachUnion(ger, BagProject(pr, "gene_set"), // for ger in p.gene_set union
                    IfThenElse(
                      And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
                        And(
                          And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"), gtfr("g_start"))),
                          And(Cmp(OpGe, gtfr("g_end"), vr("start")), Cmp(OpEq, gtfr("g_contig"), vr("contig"))))),
                      Singleton(Tuple("name" -> pr("p_name"), "burden" -> Const(1, IntType)))
                    )
                  )
                ), List("name"), List("burden")
              )
            )
        )
        )
      )
    )

  // result: {(sample: String, pathways: {(name: String, total_burden: Int)})}
  /*
      For v in Variants Union
      For g in v.genotypes Union
              {( sample := g.sample,
                      pathway := For p in Pathways Union
                              ReduceByKey([name], [burden],
                  For gtf in Gtfs Union
                  For ge in p.gene_set Union
                      If (gtf.gene_name = ge.name AND g.call != 0 AND v.start >= gtf.start AND gtf.end >= v.start AND gtf.contig = v.contig) Then
                      {( name := p.name,
                              burden := 1 )}
                  ) )}
  */

  // 5.1 {(gene_name: String, clusters := {( cluster_name: String, samples := {(sample_name: String)} )} )}
  //
  //For g in genes union
  //{(g.name, clusters :=
  //groupBy^{cluster_name}(For v in Variant union
  //if g.start <= v.start and g.end >= v.end then
  //For g in v.genotypes union
  //if g.call != 0 then {( cluster_name := “burdened”, sample_name := g.sample )}
  //else  {( cluster_name := “not burdened”, sample_name := g.sample )}) )}


  // 6.1 {(population_name: String, samples: {(name: String, variants: {(...)})} )}
  val query6_1 =
    ForeachUnion(mr, metadata, // for mr in metadata

      Singleton(Tuple("population_name" -> mr("population"), "samples" ->
        ForeachUnion(vr, variants, // For vr in Variants
          ForeachUnion(gr, BagProject(vr, "genotypes"), //        For gr in vr.genotypes union
            IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")), // if gr.sample == mr.m_sample
              Singleton(Tuple("name" -> gr("g_sample"), //          {(sample := gr.sample,
                "variants" -> // variants :=
                  ForeachUnion(gtfr, gtfs, // for gtfr in gtfs
                    IfThenElse(
                      Cmp(OpNe, gr("call"), Const(0, IntType)) && Cmp(OpGe, vr("start"), gtfr("g_start"))
                        && Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
                      // if (...) ==> {variants attributes}
                      Singleton(Tuple("contig" -> vr("contig"), "start" -> vr("start"), "reference" -> vr("reference"), "alternate" -> vr("alternate")))
                    )
                  )
              ))
            )))
      )
      ))

  /* 6.1 result:
  * For m in metadata Union
    {( population_name := m.population,
       samples := For v in Variants Union
      For g in v.genotypes Union
        {( name := g.sample,
           variants := For gtf in Gtfs Union
          If (g.sample = m.m_sample AND g.call != 0 AND v.start >= gtf.start AND gtf.end >= v.start AND gtf.contig = v.contig) Then
            {( contig := v.contig,
               start := v.start,
               reference := v.reference,
               alternate := v.alternate )} )} )}
  */
  val program = Program(Assignment(name, query6_1))
}


object Extension extends GenomicSchema {

  val name = "extension"
  val query =
    ForeachUnion(mr, metadata, // for mr in metadata

      Singleton(Tuple("sample" -> mr("m_sample"), "race" -> mr("population"), "variants" ->
        ForeachUnion(vr, variants,
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
              Singleton(Tuple("call" -> gr("call"), "start" -> vr("start"), "contig" -> vr("contig"),
                "reference" -> vr("reference"), "alternate" -> vr("alternate")))))))))
  //{sample, variants:{population, call, start, contig, reference, alternate}}
  val program = Program(Assignment(name, query))
}


object Step0 extends GenomicSchema {
  val name = "Races"
  val query = DeDup(ForeachUnion(mr, metadata,
    Singleton(Tuple("race_key" -> mr("population")))
  ))
  val program = Program(Assignment(name, query))
}


object Step1 extends GenomicSchema {

  val name = "SampleVariantsByRace"
  val (races, r) = varset(Step0.name, "r", Step0.program(Step0.name).varRef.asInstanceOf[BagExpr])

  val query =
    ForeachUnion(r, races,
      Singleton(Tuple("race" -> r("race_key"), "samples" ->
        ForeachUnion(vr, variants,
          ForeachUnion(mr, metadata,
            Singleton(Tuple("name" -> mr("m_sample"), "variants" ->
              ForeachUnion(gr, vr("genotypes").asBag,
                IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")) && Cmp(OpEq, r("race_key"), mr("population"))
                  && Cmp(OpNe, gr("call"), Const(0, IntType)),
                  ForeachUnion(gtfr, gtfs,
                    IfThenElse(
                      Cmp(OpGe, vr("start"), gtfr("g_start")) &&
                        Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
                      Singleton(Tuple("start" -> vr("start"), "contig" -> vr("contig"),
                        "reference" -> vr("reference"), "alternate" -> vr("alternate"))
                      ))))))))))))

  val p1 = Assignment(Step0.name, Step0.query.asInstanceOf[Expr])

  val program = Program(p1, Assignment(name, query))
}

object Step2 extends GenomicSchema {
  val name = "ORStep2v2"
  //    {(population_name: String, samples: {(name: String, variants: {(...)})} )}
  val (cnts, ac) = varset(Step1.name, "v2", Step1.program(Step1.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("samples").asInstanceOf[BagExpr].tp.tp)
  val query =
    ForeachUnion(ac, cnts,
      Singleton(Tuple("population_name" -> ac("population_name"), "samples" ->
        ForeachUnion(ac2, BagProject(ac, "samples"),

          Singleton(Tuple("name" -> ac2("name"), "variants" -> // variants :=
            ForeachUnion(gtfr, gtfs, // for gtfr in gtfs
              IfThenElse(
                Cmp(OpNe, ac2("call"), Const(0, IntType)) && Cmp(OpGe, ac2("start"), gtfr("g_start"))
                  && Cmp(OpGe, gtfr("g_end"), ac2("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),
                // if (...) ==> {variants attributes}
                Singleton(Tuple("contig" -> ac2("contig"), "start" -> ac2("start"), "reference" -> ac2("reference"), "alternate" -> ac2("alternate")))
              )
            )
          ))
        )
      ))
    )
  val program = Step1.program.asInstanceOf[Step2.Program].append(Assignment(name, query))
}


object Gene_Burden extends GenomicSchema {
  //  {(sample: String, genes: { (name: String, burden: Double) })}
  val name = "Gene_Burden"
  val query =
    ForeachUnion(vr, variants, // For v in Variants
      ForeachUnion(gr, BagProject(vr, "genotypes"),
        Singleton(Tuple("sample" -> gr("g_sample"),
          "genes" ->
            ReduceByKey(
              ForeachUnion(gtfr, gtfs,
                IfThenElse(

                  Cmp(OpGe, vr("start"), gtfr("g_start")) &&
                    Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),

                  Singleton(Tuple("name" -> gtfr("gene_name"),
                    "burden" ->
                      PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
                  ))
                )
              ),
              List("name"),
              List("burden")
            )
        ))
      )
    )
  val program = Program(Assignment(name, query))
}

object Pathway_Burden extends GenomicSchema {
  // {sample: String, pathway:{name: String, burden: Int}}
  val name = "Pathway_Burden"
  val query =
    ForeachUnion(vr, variants, // For v in Variants
      ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union

        Singleton(Tuple("sample" -> gr("g_sample"), //          {(sample := g.sample, genes :=
          "pathway" ->
            ForeachUnion(pr, pathways, // for p in Pathways union
              ReduceByKey( //              (sumby(name)(burden)
                ForeachUnion(gtfr, gtfs, // for gtfr in gtfs union
                  IfThenElse(Cmp(OpGe, vr("start"), gtfr("g_start")) &&
                    Cmp(OpGe, gtfr("g_end"), vr("start")) && Cmp(OpEq, gtfr("g_contig"), vr("contig")),

                    ForeachUnion(ger, BagProject(pr, "gene_set"), // for ger in p.gene_set union
                      IfThenElse(
                        Cmp(OpEq, gtfr("gene_name"), ger("name")),
                        Singleton(Tuple("name" -> pr("p_name"),
                          "burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))))
                      )
                    )
                  )
                ), List("name"), List("burden")
              )
            )
        )
        )
      )
    )
  val program = Program(Assignment(name, query))
}

object Pathway_Burden_Flatten extends GenomicSchema {
  val name = "Pathway_Burden_Flatten"
  // {sample: String, pathway:{name: String, burden: Int}} ==> {sample: String, pathway:{name: String, burden: Int}}
  val (cnts, ac) = varset(Pathway_Burden.name, "v2", Pathway_Burden.program(Pathway_Burden.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("pathway").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(ac, cnts,
      ForeachUnion(ac2, ac("pathway").asBag,
        Singleton(Tuple("sample" -> ac("sample"), "pathway_name" -> ac2("name"), "total_burden" -> ac2("burden"))))
    )
  val program = Pathway_Burden.program.asInstanceOf[Pathway_Burden_Flatten.Program].append(Assignment(name, query))
}

object Clinical_Pathway_Burden extends GenomicSchema {
  //  {sample: String, family_id: String, population: String, gender: String, pathway:{name: String, burden: Int}}
  val name = "Clinical_Burden"
  val (cnts, ac) = varset(Pathway_Burden.name, "v2", Pathway_Burden.program(Pathway_Burden.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("pathway").asInstanceOf[BagExpr].tp.tp)

  val query =
    ForeachUnion(ac, cnts,
      ForeachUnion(mr, metadata,
        IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),

          Singleton(Tuple("sample" -> ac("sample"), "family_id" -> mr("family_id"), "population" -> mr("population"), "gender" -> mr("gender"),
            "pathways" ->
              ForeachUnion(ac2, ac("pathway").asBag,
                Singleton(Tuple("pathway_name" -> ac2("name"), "total_burden" -> ac2("burden"))))
          )
          )
        )))
  val program = Pathway_Burden.program.asInstanceOf[Clinical_Pathway_Burden.Program].append(Assignment(name, query))
}

object Clinical_Pathway_Burden_Flatten extends GenomicSchema {
  //  {sample: String, family_id: String, population: String, gender: String, pathway_name: String, total_burden: Int}
  val name = "Clinical_Burden_Flatten"
  val (cnts, ac) = varset(Pathway_Burden.name, "v2", Pathway_Burden.program(Pathway_Burden.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("pathway").asInstanceOf[BagExpr].tp.tp)
  val query =
    ForeachUnion(ac, cnts,
      ForeachUnion(mr, metadata,
        IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
          ForeachUnion(ac2, ac("pathway").asBag,
            Singleton(Tuple("sample" -> ac("sample"), "family_id" -> mr("family_id"), "population" -> mr("population"), "gender" -> mr("gender"), "pathway_name" -> ac2("name"), "total_burden" -> ac2("burden"))))
        )
      )
    )
  val program = Pathway_Burden.program.asInstanceOf[Clinical_Pathway_Burden_Flatten.Program].append(Assignment(name, query))
}

object Example1 extends GenomicSchema {

  val name = "Example1"
  val query =
    ForeachUnion(mr, metadata,

      Singleton(Tuple("population_name" -> mr("population"), "samples" ->
        ForeachUnion(vr, variants,
          ForeachUnion(gr, BagProject(vr, "genotypes"),
            IfThenElse(Cmp(OpEq, gr("g_sample"), mr("m_sample")),
              Singleton(Tuple("name" -> gr("g_sample"))
              )))))
      ))
  val program = Program(Assignment(name, query))
  // {(population, samples:{(name, count)})}
}

// join pathway and gene
object pathway_by_gene extends GenomicSchema {
  val name = "pathway_by_gene"
  val query =
    ForeachUnion(pr, pathways,
      ForeachUnion(ger, BagProject(pr, "gene_set"), // for ger in p.gene_set union
        ForeachUnion(gtfr, gtfs,
          IfThenElse(
            Cmp(OpEq, gtfr("gene_name"), ger("name")),
            Singleton(Tuple("pathway_name" -> pr("p_name"), "gene_name" -> ger("name"),
              "g_start" -> gtfr("g_start"), "g_end" -> gtfr("g_end"), "g_contig" -> gtfr("g_contig")
            ))
          )
        ))
    )
  //  {pathway_name, gene_name, g_start, g_end, g_contig}
  val program = Program(Assignment(name, query))
}


// join metadata, pathway, and gene
object transpose extends GenomicSchema {
  // variants: {contig, start, reference, alternate, genotype:{g_sample, call}}
  // metadata: {m_sample, family_id, population, gender}
  // pathway_by_gene: {pathway_name, gene_name, start, end, contig}

  val (cnts, ac) = varset(pathway_by_gene.name, "v2", pathway_by_gene.program(pathway_by_gene.name).varRef.asInstanceOf[BagExpr])

  val name = "transpose"
  val query =
    ForeachUnion(mr, metadata,
      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathway_variants" ->
          ForeachUnion(vr, variants,
            ForeachUnion(gr, BagProject(vr, "genotypes"),
              IfThenElse(Cmp(OpEq, mr("m_sample"), gr("g_sample")),
                ForeachUnion(ac, cnts,
                  IfThenElse(
                    Cmp(OpGe, vr("start"), ac("g_start")) && Cmp(OpEq, ac("g_contig"), vr("contig")) && Cmp(OpGe, ac("g_end"), vr("start")),
                    Singleton(Tuple(
                      "gene_name" -> ac("gene_name"), "pathway_name" -> ac("pathway_name"),
                      "contig" -> vr("contig"), "start" -> vr("start"),
                      "alternate" -> vr("alternate"), "reference" -> vr("reference"), "call" -> gr("call"),
                      "burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
                    )
                    )
                  )
                )
              )
            )
          )
      ))
    )
  //  {sample, pathway_variants:{gene_name, pathway_name, contig, start, alternate, reference, call， burden}}
  val program = pathway_by_gene.program.asInstanceOf[transpose.Program].append(Assignment(name, query))
}

// {sample, {pathway_name, burden}}
object plan2 extends GenomicSchema {
  val name = "pathway_burden_plan2"

  val (cnts, ac) = varset(transpose.name, "v3", transpose.program(transpose.name).varRef.asInstanceOf[BagExpr])
  val ac2 = TupleVarRef("c2", ac("pathway_variants").asInstanceOf[BagExpr].tp.tp)
  val query =
    ForeachUnion(ac, cnts,
      Singleton(Tuple(
        "sample" -> ac("sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(ac2, ac("pathway_variants").asBag,
              Singleton(Tuple("pathway_name" -> ac2("pathway_name"), "burden" -> ac2("burden")))),
            List("pathway_name"),
            List("burden")
          )
      )
      )
    )
  val program = transpose.program.asInstanceOf[plan2.Program].append(Assignment(name, query))
}

// join metadata, pathway, and gene
object flatten extends GenomicSchema {
  // variants: {contig, start, reference, alternate, genotype:{g_sample, call}}
  // metadata: {m_sample, family_id, population, gender}
  // pathway_by_gene: {pathway_name, gene_name, start, end, contig}

  val (cnts, ac) = varset(pathway_by_gene.name, "v2", pathway_by_gene.program(pathway_by_gene.name).varRef.asInstanceOf[BagExpr])

  val name = "transpose"
  val query =
    ForeachUnion(mr, metadata,

      ForeachUnion(vr, variants,
        ForeachUnion(gr, BagProject(vr, "genotypes"),
          ForeachUnion(ac, cnts,
            IfThenElse(
              Cmp(OpGe, vr("start"), ac("g_start")) && Cmp(OpEq, ac("g_contig"), vr("contig")) && Cmp(OpGe, ac("g_end"), vr("start")),
              Singleton(Tuple(
                "sample" -> gr("g_sample"),
                "gene_name" -> ac("gene_name"), "pathway_name" -> ac("pathway_name"),
                "contig" -> vr("contig"), "start" -> vr("start"),
                "alternate" -> vr("alternate"), "reference" -> vr("reference"), "call" -> gr("call"),
                "burden" -> PrimitiveIfThenElse(Cmp(OpNe, gr("call"), Const(0.0, DoubleType)), Const(1.0, DoubleType), Const(0.0, DoubleType))
              )
              )
            )
          )
        )
      )
    )
  //  {sample, pathway_variants:{gene_name, pathway_name, contig, start, alternate, reference, call， burden}}
  val program = pathway_by_gene.program.asInstanceOf[flatten.Program].append(Assignment(name, query))
}

// {sample, {pathway_name, burden}}
object plan2_1 extends GenomicSchema {
  val name = "pathway_burden_plan2_1"

  val (cnts, ac) = varset(flatten.name, "v3", flatten.program(flatten.name).varRef.asInstanceOf[BagExpr])
  val query =
    ForeachUnion(mr, metadata,

      Singleton(Tuple(
        "sample" -> mr("m_sample"), "pathways" ->
          ReduceByKey(
            ForeachUnion(ac, cnts,
              IfThenElse(Cmp(OpEq, mr("m_sample"), ac("sample")),
                Singleton(Tuple("pathway_name" -> ac("pathway_name"), "burden" -> ac("burden")))
              )
            ),
            List("pathway_name"),
            List("burden"))
      ))
    )
  val program = flatten.program.asInstanceOf[plan2_1.Program].append(Assignment(name, query))
}