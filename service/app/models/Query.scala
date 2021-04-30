package models

import java.util.UUID

final case class Query(_id: Option[UUID], title: String, body: String)

object Query {
  import play.api.libs.json._

  implicit val queryFormat: OFormat[Query] = Json.format[Query]
}
