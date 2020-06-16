package framework.examples


import framework.common._

object GenomicQuery1 extends GenomicSchema {

    val name = "GenomicQuery1"

    // this query just iterates over the variant set
    val query =
        ForeachUnion(vr, variants,                                    // For v in Variants
            Singleton(Tuple("contig" -> vr("contig"),                 //   {( contig := v.contig,
                    "start" -> vr("start"),                           //      start := v.start,
                    "reference" -> vr("reference"),                   //       reference := v.reference,
                    "alternate" -> vr("alternate"),                   //       alternate := v.alternate,
                    "genotypes" ->                                    //       genotypes :=
                        ForeachUnion(gr, BagProject(vr, "genotypes"), //        For g in v.genotypes union
                        Singleton(Tuple("sample" -> gr("sample"),     //          {(sample := g.sample,
                                        "call" -> gr("call")))        //            call := g.call )}
            ))))                                                      //    )}

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
        ForeachUnion(vr, variants,                                    // For v in Variants
            ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For g in v.genotypes union
                Singleton(Tuple("sample" -> gr("sample"),       //          {(sample := g.sample,
                    "genes" ->                                   //              genes := sumby
                        ReduceByKey(
                            ForeachUnion(gtfr, gtfs,
                                IfThenElse(
                                    And(
                                        And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("g_start"))),
                                        And(Cmp(OpGe, gtfr("g_end"),vr("start")), Cmp(OpEq, gtfr("g_contig"),vr("contig")))),
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
        ForeachUnion(vr, variants,                                    // For v in Variants
            ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For g in v.genotypes union
                Singleton(Tuple("sample" -> gr("sample"),       //          {(sample := g.sample, genes :=
                    "pathway" ->
                    ForeachUnion(pr, pathways,                                  // for p in Pathways union
                        Singleton(Tuple("name" -> pr("name"),       // pathway := {( name := p.name,
                            "genes" -> ReduceByKey(                       //              (sumby(name)(burden)
                                ForeachUnion(gtfr, gtfs,                                    // for gtfr in gtfs union
                                    ForeachUnion(ger, BagProject(pr, "gene_set"),               // for ger in p.gene_set union
                                        IfThenElse(
                                            And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
                                                And(
                                                    And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("g_start"))),
                                                    And(Cmp(OpGe, gtfr("g_end"),vr("start")), Cmp(OpEq, gtfr("g_contig"),vr("contig"))))),
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
////                            Singleton(Tuple("name" -> pr("name"),       // pathway := {( name := p.name,
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
//                                    "Pathways" -> Singleton(Tuple("name" -> pr("name"),
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
        ForeachUnion(vr, variants,                                    // For v in Variants
            ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For g in v.genotypes union
                Singleton(Tuple("sample" -> gr("sample"),       //          {(sample := g.sample, genes :=
                    "pathway" ->
                        ForeachUnion(pr, pathways,                                  // for p in Pathways union
                            ReduceByKey(                       //              (sumby(name)(burden)
                                ForeachUnion(gtfr, gtfs,                                    // for gtfr in gtfs union
                                    ForeachUnion(ger, BagProject(pr, "gene_set"),               // for ger in p.gene_set union
                                        IfThenElse(
                                            And(Cmp(OpEq, gtfr("gene_name"), ger("name")),
                                                And(
                                                    And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("g_start"))),
                                                    And(Cmp(OpGe, gtfr("g_end"),vr("start")), Cmp(OpEq, gtfr("g_contig"),vr("contig"))))),
                                            Singleton(Tuple("name" -> pr("name"), "burden" -> Const(1, IntType)))
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
        ForeachUnion(mr, metadata,                                              // for mr in metadata

            Singleton(Tuple("population_name" -> mr("population"), "samples" ->
                    ForeachUnion(vr, variants,                                    // For vr in Variants
                        ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For gr in vr.genotypes union
                            IfThenElse(Cmp(OpEq, gr("sample"), mr("m_sample")),         // if gr.sample == mr.m_sample
                                Singleton(Tuple("name" -> gr("sample"),       //          {(sample := gr.sample,
                                    "variants" ->                                           // variants :=
                                            ForeachUnion(gtfr, gtfs,                        // for gtfr in gtfs
                                                IfThenElse(
                                                    Cmp(OpNe, gr("call"), Const(0, IntType)) && Cmp(OpGe, vr("start"),gtfr("g_start"))
                                                            && Cmp(OpGe, gtfr("g_end"),vr("start")) && Cmp(OpEq, gtfr("g_contig"),vr("contig")),
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


object Step1 extends GenomicSchema{

    val name = "Step1"
    // 6.1 {(population_name: String, samples: {(name: String, variants: {(...)})} )}
    val query =
        ForeachUnion(mr, metadata,                                              // for mr in metadata

            Singleton(Tuple("population_name" -> mr("population"), "samples" ->
                    ForeachUnion(vr, variants,                                    // For vr in Variants
                        ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For gr in vr.genotypes union
                            IfThenElse(Cmp(OpEq, gr("sample"), mr("m_sample")),         // if gr.sample == mr.m_sample
                                Singleton(Tuple("name" -> gr("sample"), "call" -> gr("call"), "start" ->vr("start"), "contig" -> vr("contig"),
                                    "reference" -> vr("reference"), "alternate" -> vr("alternate"))
                                )))))))
    val program = Program(Assignment(name, query))
    // {(population, samples:{(name, call, start, contig)})}
}



object Step2 extends GenomicSchema {
    val name = "ORStep2"

    val (cnts, ac) = varset(Step1.name, "v2", Step1.program(Step1.name).varRef.asInstanceOf[BagExpr])
    val ac2 = TupleVarRef("c2", ac("samples").asInstanceOf[BagExpr].tp.tp)
    val query =
        ForeachUnion(ac, cnts,
            Singleton(Tuple("population_name" -> ac("population_name"), "samples" ->
                ForeachUnion(ac2, BagProject(ac, "samples"),

                    Singleton(Tuple("name" -> ac2("name"), "variants" ->                                           // variants :=
                            ForeachUnion(gtfr, gtfs,                        // for gtfr in gtfs
                                IfThenElse(
                                    Cmp(OpNe, ac2("call"), Const(0, IntType)) && Cmp(OpGe, ac2("start"),gtfr("g_start"))
                                            && Cmp(OpGe, gtfr("g_end"),ac2("start")) /*&& Cmp(OpEq, gtfr("g_contig"),vr("contig"))*/,
                                    // if (...) ==> {variants attributes}
                                    Singleton(Tuple("contig" -> ac2("contig"), "start" -> ac2("start"), "reference" -> ac2("reference"), "alternate" -> ac2("alternate")))
                                )
                            )))
                )

            ))
        )
    val program = Step1.program.asInstanceOf[Step2.Program].append(Assignment(name, query))
}
