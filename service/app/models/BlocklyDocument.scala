package models

import play.api.libs.json.{Json, OFormat}
import java.util.UUID

final case class BlocklyDocument(_id: Option[UUID], name: String, xmlDocument: String)

object BlocklyDocument {

  implicit val blocklyDocFormat: OFormat[BlocklyDocument] = Json.format[BlocklyDocument]
}
