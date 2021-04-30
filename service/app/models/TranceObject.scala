package models

import java.util.UUID

/**
  * Model for the Object to be sent to trance webapp to define databases object information
  */
final case class TranceObject(
                       _id: Option[UUID],
                       name: String,
                       abr: Option[String],
                       columns: List[Column]
                       )

final case class Column(name: String)

object Column {
  import play.api.libs.json._
    implicit val columnFormat: OFormat[Column] = Json.format[Column]
}

final case class CollectionColumn (
                    name: String,
                    abr: Option[String],
                    columns: List[Column]
                                  )
object CollectionColumn {
  import play.api.libs.json._
  implicit val columnFormat: OFormat[CollectionColumn] = Json.format[CollectionColumn]
}

object TranceObject {
  import play.api.libs.json._

  implicit val tableObjectFormat: OFormat[TranceObject] = Json.format[TranceObject]

}