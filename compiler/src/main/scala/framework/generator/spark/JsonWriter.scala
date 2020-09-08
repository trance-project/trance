package framework.generator.spark

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write
case class Paragraph(text: String, user: String = "anonymous")

case class noteParam()
case class angularObject()
case class noteForm()
case class ConfigSchema(isZeppelinNotebookCronEnable: Boolean = false, looknfeel: String = "default", personalizedMode: String = "false")
case class inf()

case class TheRootSchema(
  paragraphs: List[Paragraph],
  name: String = "notebook",
  id: String = "2FDG6Z2M5",
  defaultInterpreterGroup: String = "spark",
  version: String = "0.9.0-preview1",
  path: String = "/notbook/"
)

class JsonWriter {

  def buildZepplin(app_name: String, texts: ArrayBuffer[String]): String = {

    val name = "example1"
    val paragraphs = new ArrayBuffer[Paragraph]()

    texts.foreach(
      m => paragraphs.append(Paragraph(m))
    )

    val p1 = TheRootSchema(paragraphs.toList, name)
    implicit val formats = DefaultFormats
    val jsonString = write(p1)
    println(jsonString)
    jsonString
  }
}

