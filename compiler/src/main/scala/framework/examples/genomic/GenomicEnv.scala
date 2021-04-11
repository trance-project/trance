package framework.examples.genomic

import framework.common._
import framework.examples.{Query, Environment}
import framework.plans._
import framework.loader.csv._


class GenomicEnv(init_capacity: Int) extends Environment {
	
	val name = "GenomicEnv"

	val capacity: Int = init_capacity

	val dtp = new DriverGene{ val name: String = ""; val program: Program = Program(Nil)}

  	val tbls = Map("occurrences" -> dtp.occurmids.tp, 
                  "copynumber" -> dtp.copynum.tp, 
                  "samples" -> dtp.samples.tp)

	override def setup(shred: Boolean = false, skew: Boolean = false, cache: Boolean = false): String = {
		if (shred){
  		  s"""|val samples = spark.table("samples")
	          |val IBag_samples__D = samples
	          |${if (cache) "IBag_samples__D.cache; IBag_samples__D.count" else ""}
	          |
	          |val copynumber = spark.table("copynumber")
	          |val IBag_copynumber__D = copynumber
	          |${if (cache) "IBag_copynumber__D.cache; IBag_copynumber__D.count" else ""}
	          |
	          |val odict1 = spark.table("odict1")
	          |val IBag_occurrences__D = odict1
	          |${if (cache) "IBag_copynumber__D.cache; IBag_copynumber__D.count" else ""}
	          |
	          |// issue with partial shredding here
	          |val odict2 = spark.table("odict2").drop("flags")
	          |val IDict_occurrences__D_transcript_consequences = odict2
	          |${if (cache) "IDict_occurrences__D_transcript_consequences.cache" else ""}
	          |${if (cache) "IDict_occurrences__D_transcript_consequences.count" else ""}
	          |
	          |val odict3 = spark.table("odict3")
	          |val IDict_occurrences__D_transcript_consequences_consequence_terms = odict3
	          |${if (cache) "IDict_occurrences__D_transcript_consequences_consequence_terms.cache" else ""}
	          |${if (cache) "IDict_occurrences__D_transcript_consequences_consequence_terms.count" else ""}
	          |""".stripMargin
		}else{
  		  s"""|val samples = spark.table("samples")
	          |${if (cache) "samples.cache; samples.count" else ""}
	          |
	          |val copynumber = spark.table("copynumber")
	          |${if (cache) "copynumber.cache; copynumber.count" else ""}
	          |
	          |val occurrences = spark.table("occurrences")
	          |${if (cache) "occurrences.cache; occurrences.count" else ""}
	          |""".stripMargin
		}
	}

	val queries: Vector[Query] = Vector(TestBaseQuery, TestBaseQuery2, TestBaseQuery3)

	val plans: Vector[(CExpr, Int)] = queries.map(q => q.optimized(optLevel, schema).asInstanceOf[CExpr]).zipWithIndex

	val cacheStrategy: CacheFactory = new CacheFactory(plans, capacity)

}