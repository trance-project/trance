package framework.plans

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write

object JsonWriter {

	def getJsonString(plan: CExpr): String = {
		implicit val formats = DefaultFormats
		write(plan)
	}

}

object JsonWriterTest extends App {

	JsonWriter.getJsonString()

}