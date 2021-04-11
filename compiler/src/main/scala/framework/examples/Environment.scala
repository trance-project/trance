package framework.examples

import framework.common._
import framework.plans._
import framework.loader.csv._

trait Environment {

	val name: String

	val schema: Schema = Schema()

	val optLevel: Int = 1

	val capacity: Int

	val tbls: Map[String, Type]

	def setup(shred: Boolean = false, skew: Boolean = false, cache: Boolean = false): String

	val queries: Vector[Query]

	val plans: Vector[(CExpr, Int)] 

	val cacheStrategy: CacheFactory

}