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
                                        And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
                                        And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig")))),
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
                                                    And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
                                                    And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig"))))),
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
                                                    And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
                                                    And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig"))))),
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
        ForeachUnion(mr, metadata,

            Singleton(Tuple("population_name" -> mr("population"), "samples" ->
                ForeachUnion(vr, variants,                                    // For v in Variants
                ForeachUnion(gr, BagProject(vr, "genotypes"),       //        For g in v.genotypes union
                    Singleton(Tuple("name" -> gr("sample"),       //          {(sample := g.sample,
                        "variants" ->
                        ForeachUnion(gtfr, gtfs,
                            IfThenElse(
                                And(Cmp(OpEq, gr("sample"), mr("m_sample")),And(And(Cmp(OpNe, gr("call"), Const(0, IntType)), Cmp(OpGe, vr("start"),gtfr("start"))),
                                    And(Cmp(OpGe, gtfr("end"),vr("start")), Cmp(OpEq, gtfr("contig"),vr("contig"))))),
                                Singleton(Tuple("contig" -> vr("contig"), "start" -> vr("start"), "reference" -> vr("reference"), "alternate" -> vr("alternate")))
                            )
                        )
                    ))
                )))
            )
        )

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

