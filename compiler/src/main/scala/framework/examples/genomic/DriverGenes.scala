package framework.examples.genomic

import framework.common._
import framework.examples.Query

trait Occurrence extends Vep {

  val occurrence_type = TupleType((vcf_vep_type.attrTps - "genotypes") ++
    Map("donorId" -> StringType, "aliquot_id" -> StringType))

}

trait Gistic {

  val sampleType = TupleType("gistic_sample" -> StringType, 
    "focal_score" -> DoubleType)

  val gisticType = TupleType("gistic_gene" -> StringType, "cytoband" -> StringType,
    "gistic_samples" -> BagType(sampleType))

}

trait StringNetwork {

  val edge = TupleType("edge_protein" -> StringType, "neighborhood" -> IntType,
   "neighborhood_transferred" -> IntType, "fusion" -> IntType, "cooccurence" -> IntType,
   "homology" -> IntType, "coexpression" -> IntType, "coexpression_transferred" -> IntType,
   "experiments" -> IntType, "experiments_transferred" -> IntType,
   "database" -> IntType, "database_transferred" -> IntType,
  "textmining" -> IntType, "textmining_transferred" -> IntType, "combined_score" -> IntType)

  val node = TupleType("node_protein" -> StringType, "edges" -> BagType(edge))
}

trait DriverGene extends Query with Occurrence with Gistic with StringNetwork {

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    s"""|//TODO""".stripMargin
  }

  val occurrences = BagVarRef("occurrences", BagType(occurrence_type))
  val or = TupleVarRef("o", occurrence_type)
  val ar = TupleVarRef("a", transcript)
  val cr = TupleVarRef("c", element)

  val gistic = BagVarRef("gistic", BagType(gisticType))
  val gr = TupleVarRef("g", gisticType)
  val sr = TupleVarRef("s", sampleType)

}

object HybridMatrix extends DriverGene {

  val name = "HybridMatrix"
  
  val query = ForeachUnion(or, occurrences,
      Singleton(Tuple("donorId" -> or("donorId"), 
        "genes" -> 
        ReduceByKey(
            ForeachUnion(ar, BagProject(or, "transcript_consequences"),
              ForeachUnion(gr, gistic, 
                IfThenElse(Cmp(OpEq, ar("gene_id"), gr("gistic_gene")),
                  ForeachUnion(sr, BagProject(gr, "gistic_samples"),
                    IfThenElse(Cmp(OpEq, sr("gistic_sample"), or("aliquot_id")),
                      ForeachUnion(cr, BagProject(ar, "consequence_terms"),
                          Singleton(Tuple("gene_id" -> ar("gene_id"),
                            "hybrid_measurement" -> 
                            ar("biotype").asNumeric * ar("impact").asNumeric *
                            (cr("element").asNumeric * sr("focal_score").asNumeric)))))))))
            ,List("gene_id"),
            List("hybrid_measurement")))))

  val program = Program(Assignment(name, query))

}

