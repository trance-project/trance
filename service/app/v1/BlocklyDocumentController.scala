package v1

import io.swagger.annotations.{Api, ApiImplicitParam, ApiImplicitParams, ApiOperation, ApiResponse, ApiResponses}
import models.{BlocklyDocument, BlocklyDocumentRepository, TranceObject}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Api(value = "/blockly")
class BlocklyDocumentController @Inject()(
                                         cc: ControllerComponents,
                                         blocklyDocumentRepository: BlocklyDocumentRepository
                                         ) extends AbstractController (cc){

  @ApiOperation(
    value = "Find all Blockly queries",
    response = classOf[BlocklyDocument],
    responseContainer = "List"
  )
  def getAll = Action.async {
    blocklyDocumentRepository.getAll.map{ blocklyDocument =>
      Ok(Json.toJson(blocklyDocument))
    }
  }

  @ApiOperation(
    value = "Get a Blockly Document ",
    response = classOf[BlocklyDocument]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Blockly Document not found")
  ))
  def getBlocklyDocument(id: UUID) =
    Action.async{ reg =>
      blocklyDocumentRepository.getSingleItem(id).map{ maybeBlocklyDocument =>
        maybeBlocklyDocument.map{ blocklyDocument =>
          Ok(Json.toJson(blocklyDocument))
        }.getOrElse(NotFound)
      }
    }

  @ApiOperation(
    value = "Add a new Blockly document to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Blockly document format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Blockly document in Json Format", required = true, dataType = "models.BlocklyDocument", paramType = "body")
  ))
  def createBlocklyDocument = Action.async(parse.json) {
    _.body.validate[BlocklyDocument].map { blocklyDocument =>
      blocklyDocumentRepository.addEntity( blocklyDocument).map{_ =>
        Created
      }
    }.getOrElse(Future.successful(BadRequest("Invalid Blockly Document format")))
  }

  @ApiOperation(
    value = "Update a Blockly Document",
    response = classOf[TranceObject]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Blockly Document format")
  )
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The updated Blockly Document, in Json Format", required = true, dataType = "models.BlocklyDocument", paramType = "body")
  )
  )
  def updateBlocklyDocument(id: UUID) =
    Action.async(parse.json) {
      req =>
        req.body.validate[BlocklyDocument].map { blocklyDocument =>
          blocklyDocumentRepository.updateEntity(id, blocklyDocument).map {
            case Some(blocklyDocument) => Ok(Json.toJson(blocklyDocument))
            case _ => NotFound
          }
        }.getOrElse(Future.successful(BadRequest("Invalid Json")))
    }

  @ApiOperation(
    value = "Delete a BlocklyDocument",
    response = classOf[BlocklyDocument]
  )
  def delete(id: UUID) = Action.async{
    req => blocklyDocumentRepository.deleteEntity(id).map {
      case Some(blocklyDocument) => Ok(Json.toJson(blocklyDocument))
      case _ => NotFound
    }
  }

}
