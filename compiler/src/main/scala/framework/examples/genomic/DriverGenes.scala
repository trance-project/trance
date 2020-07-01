package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait Occurrence extends Vep {

  val occurrence_type = TupleType(
      "oid" -> StringType,
      "donorId" -> StringType, 
      "vend" -> LongType,
      "projectId" -> StringType, 
      "vstart" -> LongType,
      "Reference_Allele" -> StringType,
      "Tumor_Seq_Allele1" -> StringType,
      "Tumor_Seq_Allele2" -> StringType,
      "chromosome" -> StringType,
      "allele_string" -> StringType,
      "assembly_name" -> StringType,
      "end" -> LongType,
      "vid" -> StringType,
      "input" -> StringType,
      "most_severe_consequence" -> StringType,
      "seq_region_name" -> StringType, 
      "start" -> LongType,
      "strand" -> LongType,
      "transcript_consequences" -> BagType(transcriptQuant)
    )

}

trait Gistic {

  val sampleType = TupleType("gistic_sample" -> StringType, 
    "focal_score" -> LongType)

  val gisticType = TupleType("gistic_gene" -> StringType, "cytoband" -> StringType,
    "gistic_gene_id" -> LongType, "gistic_samples" -> BagType(sampleType))

}

trait StringNetwork {

  val edgeType = TupleType("edge_protein" -> StringType, "neighborhood" -> IntType,
   "neighborhood_transferred" -> IntType, "fusion" -> IntType, "cooccurence" -> IntType,
   "homology" -> IntType, "coexpression" -> IntType, "coexpression_transferred" -> IntType,
   "experiments" -> IntType, "experiments_transferred" -> IntType,
   "database" -> IntType, "database_transferred" -> IntType,
  "textmining" -> IntType, "textmining_transferred" -> IntType, "combined_score" -> IntType)

  val nodeType = TupleType("node_protein" -> StringType, "edges" -> BagType(edgeType))

}

trait GeneExpression {

  val geneExprType = TupleType("expr_gene" -> StringType, "fpkm" -> DoubleType)
  val sampleExprType = TupleType("expr_sample" -> StringType, "gene_expression" -> BagType(geneExprType))

}

trait Biospecimen {

  val biospecOtype = List(
    ("bcr_patient_uuid", StringType), ("bcr_sample_barcode", StringType), 
    ("bcr_aliquot_barcode", StringType), ("bcr_aliquot_uuid", StringType), 
    ("biospecimen_barcode_bottom", StringType), ("center_id", StringType), 
    ("concentration", DoubleType), ("date_of_shipment", StringType), 
    ("is_derived_from_ffpe", StringType), ("plate_column", IntType),
    ("plate_id", StringType), ("plate_row", StringType), 
    ("quantity", DoubleType), ("source_center", IntType), 
    ("volume", DoubleType))
  val biospecType = TupleType(biospecOtype.toMap)

}

trait DriverGene extends Query with Occurrence with Gistic with StringNetwork with GeneExpression with Biospecimen {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    s"""|val occurrences = spark.read.json("file:///nfs_qc4/genomics/gdc/somatic/dataset/").as[Occurrence]
		|occurrences.cache
		|occurrences.count
		|val gistic = spark.read.json("file:///nfs_qc4/genomics/gdc/gistic/dataset/").as[Gistic]
		|gistic.cache
		|gistic.count
		|val biospecLoader = new BiospecLoader(spark)
		|val biospec = biospecLoader.load("/nfs_qc4/genomics/gdc/biospecimen/")
		|biospec.cache
		|biospec.count
		|val consequenceLoader = new ConsequenceLoader(spark)
		|val conseqmap = spark.sparkContext.broadcast(consequenceLoader.read("/nfs_qc4/genomics/calc_variant_conseq.txt"))
                |val quantifyImpact = udf { s: String => s match { case null _ => 0.1; case _ => conseqmap.value.getOrElse(s, 0.1) }} 
		|val quantifyConsequence = udf { s: String => s match {
                |  case null => .1
                |  case "HIGH" => .8
		|  case "MODERATE" => .5
		|  case "LOW" => .3
		|  case "MODIFIER" => .15
                |  case _ => .1
		|}}
		|""".stripMargin
  }

  val occurrences = BagVarRef("occurrences", BagType(occurrence_type))
  val or = TupleVarRef("o", occurrence_type)
  val ar = TupleVarRef("a", transcriptQuant)
  val cr = TupleVarRef("c", element)

  val gistic = BagVarRef("gistic", BagType(gisticType))
  val gr = TupleVarRef("g", gisticType)
  val sr = TupleVarRef("s", sampleType)

  val network = BagVarRef("network", BagType(nodeType))
  val nr = TupleVarRef("n", nodeType)
  val er = TupleVarRef("e", edgeType)

  val expression = BagVarRef("expression", BagType(sampleExprType))
  val sexpr = TupleVarRef("sexpr", sampleExprType)
  val gexpr = TupleVarRef("gexpr", geneExprType)

  val biospec = BagVarRef("biospec", BagType(biospecType))
  val br = TupleVarRef("b", biospecType)

  def projectTuple(tr: TupleVarRef, nbag:Map[String, TupleAttributeExpr], omit: List[String] = Nil): BagExpr =
    Singleton(Tuple(tr.tp.attrTps.withFilter(f =>
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) ++ nbag))

}

object HybridBySample extends DriverGene {

  val name = "HybridBySample"
 
  val mapped = ForeachUnion(or, occurrences, 
    ForeachUnion(br, biospec, 
      IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
        projectTuple(or, Map("aliquot_id" -> Project(br, "bcr_aliquot_uuid"))))))

  val (maps, mapsRef) = varset("occurrence_mapped", "mr", mapped)

  val query = ForeachUnion(mapsRef, maps,
    Singleton(Tuple("hybrid_sample" -> mapsRef("donorId"), "hybrid_aliquot" -> mapsRef("aliquot_id"),
        "hybrid_genes" -> 
        ReduceByKey(
            ForeachUnion(ar, BagProject(mapsRef, "transcript_consequences"),
              ForeachUnion(gr, gistic, 
                IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
                  ForeachUnion(sr, BagProject(gr, "gistic_samples"),
                    IfThenElse(Cmp(OpEq, sr("gistic_sample"), mapsRef("aliquot_id")),
                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
                          Singleton(Tuple("hybrid_gene_id" -> ar("gene_id"),
                            "hybrid_score" -> 
                            //ar("biotype").asNumeric * 
                            Udf("quantifyImpact", ar("impact").asPrimitive, DoubleType) * 
							Udf("quantifyConsequence", cr("element").asPrimitive, DoubleType) * 
							sr("focal_score").asNumeric))))))))
            ,List("hybrid_gene_id"),
            List("hybrid_score")))))

  val program = Program(Assignment(maps.name, mapped), Assignment(name, query))

}

object SampleNetwork extends DriverGene {

  val name = "SampleNetwork"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid, 
      Singleton(Tuple("network_sample" -> hmr("hybrid_sample"), "network_genes" -> 
          ReduceByKey(
            ForeachUnion(gene, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(nr, network, 
                ForeachUnion(er, BagProject(nr, "edges"),
                  IfThenElse(Cmp(OpEq, er("edge_protein"), gene("hybrid_gene_id")),
                    Singleton(Tuple("network_gene_id" -> gene("hybrid_gene_id"), 
                      "distance" -> er("combined_score").asNumeric * gene("hybrid_score").asNumeric
                      )))))),
            List("network_gene_id"),
            List("distance")))))

  val program = HybridBySample.program.asInstanceOf[SampleNetwork.Program].append(Assignment(name, query))

}

object EffectBySample extends DriverGene {

  val name = "EffectBySample"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)
  val (snetwork, snr) = varset(SampleNetwork.name, "sn", SampleNetwork.program(SampleNetwork.name).varRef.asInstanceOf[BagExpr])
  val gene2 = TupleVarRef("ngene", snr.tp("network_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid,
      ForeachUnion(snr, snetwork, 
        IfThenElse(Cmp(OpEq, hmr("hybrid_sample"), snr("network_sample")),
          Singleton(Tuple("effect_sample" -> hmr("hybrid_sample"), "effect_genes" -> 
            ForeachUnion(gene1, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(gene2, BagProject(snr, "network_genes"),
                IfThenElse(Cmp(OpEq, gene1("hybrid_gene_id"), gene2("network_gene_id")),
                  Singleton(Tuple("effect_gene_id" -> gene1("hybrid_gene_id"), 
                    "effect" -> gene1("hybrid_score").asNumeric * gene2("distance").asNumeric))))))))))

  val program = SampleNetwork.program.asInstanceOf[EffectBySample.Program].append(Assignment(name, query))

}

object EffectBySample2 extends DriverGene {

  val name = "EffectBySample2"

  val (hybrid, hmr) = varset(HybridBySample.name, "hm", HybridBySample.program(HybridBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("hgene", hmr.tp("hybrid_genes").asInstanceOf[BagType].tp)
  val (snetwork, snr) = varset(SampleNetwork.name, "sn", SampleNetwork.program(SampleNetwork.name).varRef.asInstanceOf[BagExpr])
  val gene2 = TupleVarRef("ngene", snr.tp("network_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(hmr, hybrid,
      Singleton(Tuple("effect_sample" -> hmr("hybrid_sample"), "effect_genes" -> 
        ForeachUnion(snr, snetwork, 
          IfThenElse(Cmp(OpEq, hmr("hybrid_sample"), snr("network_sample")),
            ForeachUnion(gene1, BagProject(hmr, "hybrid_genes"),
              ForeachUnion(gene2, BagProject(snr, "network_genes"),
                IfThenElse(Cmp(OpEq, gene1("hybrid_gene_id"), gene2("network_gene_id")),
                  Singleton(Tuple("effect_gene_id" -> gene1("hybrid_gene_id"), 
                    "effect" -> gene1("hybrid_score").asNumeric * gene2("distance").asNumeric))))))))))

  val program = SampleNetwork.program.asInstanceOf[EffectBySample2.Program].append(Assignment(name, query))

}

object ConnectionBySample extends DriverGene {

  val name = "ConnectionBySample"

  val (effect, emr) = varset(EffectBySample.name, "em", EffectBySample.program(EffectBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("egene", emr.tp("effect_genes").asInstanceOf[BagType].tp)

  val query = ForeachUnion(emr, effect, 
    ForeachUnion(sexpr, expression, 
      IfThenElse(Cmp(OpEq, emr("effect_sample"), sexpr("expr_sample")), 
        Singleton(Tuple("connection_sample" -> emr("effect_sample"), "connect_genes" -> 
          ForeachUnion(gene1, BagProject(emr, "effect_genes"),
            ForeachUnion(gexpr, BagProject(sexpr, "gene_expression"),
              IfThenElse(Cmp(OpEq, gene1("effect_gene_id"), gexpr("expr_gene")),
                Singleton(Tuple("connect_gene" -> gene1("effect_gene_id"), 
                  "gene_connectivity" -> gene1("effect").asNumeric * gexpr("fpkm").asNumeric))))))))))

  val program = EffectBySample.program.asInstanceOf[ConnectionBySample.Program].append(Assignment(name, query))

}

object GeneConnectivity extends DriverGene {

  val name = "GeneConnectivity"

  val (connect, cmr) = varset(ConnectionBySample.name, "em", ConnectionBySample.program(ConnectionBySample.name).varRef.asInstanceOf[BagExpr])
  val gene1 = TupleVarRef("cgene", cmr.tp("connect_genes").asInstanceOf[BagType].tp)

  val query = ReduceByKey(ForeachUnion(cmr, connect, 
      ForeachUnion(gene1, BagProject(cmr, "connect_genes"),
        Singleton(Tuple("gene_id" -> gene1("connect_gene"), 
          "connectivity" -> gene1("gene_connectivity"))))),
        List("gene_id"),
        List("connectivity"))

  val program = ConnectionBySample.program.asInstanceOf[GeneConnectivity.Program].append(Assignment(name, query))

}





