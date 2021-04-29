package models

import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.play.json.compat.json2bson.{toDocumentReader, toDocumentWriter}

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

/**
  * Model for the Object to be sent to trance webapp to define databases object information
  */
final case class TranceObject(
                       _id: Option[UUID],
                       name: String,
                       abr: Option[String],
//                       column: List[Column]
                       )

final case class Column(name: String, children: TranceObject)

object TranceObject {
  import play.api.libs.json._

  implicit val tableObjectFormat: OFormat[TranceObject] = Json.format[TranceObject]
//  implicit val columnFormat: OFormat[Column] = Json.format[Column]
}

class TranceObjectRepository @Inject()(
                                implicit ec: ExecutionContext,
                                reactiveMongoApi: ReactiveMongoApi
                              ){

  //def object name in database
  private def collection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection("trance_objects"))

  def getAll: Future[Seq[TranceObject]] =
    collection.flatMap(_.find(BSONDocument.empty).
      cursor[TranceObject]().collect[Seq](100))

  def getSingleItem(id: UUID): Future[Option[TranceObject]] =
    collection.flatMap(_.find(BSONDocument("_id" -> id)).one[TranceObject])

  def addEntity(tranceObject: TranceObject): Future[WriteResult] =
    collection.flatMap(_.insert.one(
      tranceObject.copy(_id = Some(UUID.randomUUID()))))

  def updateEntity(id: UUID, tranceObject: TranceObject): Future[Option[TranceObject]] = {
    val updateModifier = BSONDocument (
      f"$$set" -> BSONDocument(
        "name" -> tranceObject.name,
        "abr" -> tranceObject.abr,
//        "columns" -> tranceObject.column. todo update a list collection type in mongo
      )
    )
    collection.flatMap(_.findAndUpdate(
      selector = BSONDocument("_id" -> id),
      update = updateModifier,
      fetchNewObject = true).map(_.result[TranceObject])
    )
  }

  def deleteEntity(id: UUID): Future[Option[TranceObject]] =
    collection.flatMap(_.findAndRemove(
      selector = BSONDocument("_id" -> id)).map(_.result[TranceObject]))
}