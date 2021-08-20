package v1.repository

import models.BlocklyDocument
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.play.json.compat.json2bson.{toDocumentReader, toDocumentWriter}

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

class BlocklyDocumentRepository @Inject() (implicit ec: ExecutionContext,
                                           reactiveMongoApi: ReactiveMongoApi){


  //def object name in database
  private def collection: Future[BSONCollection] =
    reactiveMongoApi.database.map(_.collection("blockly_document"))

  def getAll: Future[Seq[BlocklyDocument]] =
    collection.flatMap(_.find(BSONDocument.empty).
      cursor[BlocklyDocument]().collect[Seq](100))

  def getSingleItem(id: UUID): Future[Option[BlocklyDocument]] =
    collection.flatMap(_.find(BSONDocument("_id" -> id)).one[BlocklyDocument])

  def addEntity(blocklyDocument: BlocklyDocument): Future[WriteResult] =
    collection.flatMap(_.insert.one(
      blocklyDocument.copy(_id = Some(UUID.randomUUID()))))

  def updateEntity(id: UUID, blocklyDocument: BlocklyDocument): Future[Option[BlocklyDocument]] = {
    val updateModifier = BSONDocument (
      f"$$set" -> BSONDocument(
        "name" -> blocklyDocument.name,
        "xmlDocument" -> blocklyDocument.xmlDocument,
        "lastRunQueryURL" -> blocklyDocument.lastRunQueryURL
      )
    )
    collection.flatMap(_.findAndUpdate(
      selector = BSONDocument("_id" -> id),
      update = updateModifier,
      fetchNewObject = true).map(_.result[BlocklyDocument])
    )
  }

  def deleteEntity(id: UUID): Future[Option[BlocklyDocument]] =
    collection.flatMap(_.findAndRemove(
      selector = BSONDocument("_id" -> id)).map(_.result[BlocklyDocument]))
}
