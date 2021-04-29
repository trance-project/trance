package models

import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.play.json.compat.json2bson.{toDocumentReader, toDocumentWriter}

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

final case class Query(_id: Option[UUID], title: String, body: String)

object Query {
  import play.api.libs.json._

  implicit val queryFormat: OFormat[Query] = Json.format[Query]
}

class QueryRepository  @Inject()(
                                  implicit ec: ExecutionContext,
                                  reactiveMongoApi: ReactiveMongoApi
                                ) {

  //def object name in database
  private def collection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection("query"))

  def getAll: Future[Seq[Query]] =
    collection.flatMap(_.find(BSONDocument.empty).
      cursor[Query]().collect[Seq](100))

  def getSingleItem(id: UUID): Future[Option[Query]] =
    collection.flatMap(_.find(BSONDocument("_id" -> id)).one[Query])

  def addEntity(query: Query): Future[WriteResult] =
    collection.flatMap(_.insert.one(
      query.copy(_id = Some(UUID.randomUUID()))))

  def updateEntity(id: UUID, query: Query): Future[Option[Query]] = {
    val updateModifier = BSONDocument (
      f"$$set" -> BSONDocument(
        "title" -> query.title,
        "body" -> query.body
      )
    )
    collection.flatMap(_.findAndUpdate(
      selector = BSONDocument("_id" -> id),
      update = updateModifier,
      fetchNewObject = true).map(_.result[Query])
    )
  }

  def deleteEntity(id: UUID): Future[Option[Query]] =
    collection.flatMap(_.findAndRemove(
      selector = BSONDocument("_id" -> id)).map(_.result[Query]))
}
