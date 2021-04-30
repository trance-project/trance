package v1.controllers

import io.swagger.annotations._
import models.{TranceObject}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}
import v1.repository.TranceObjectRepository

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Api(value = "/tranceObject")
class TranceObjectController @Inject()(
                                        cc: ControllerComponents,
                                        tranceObjectRepository: TranceObjectRepository
                                      ) extends AbstractController (cc){
  @ApiOperation(
    value = "Find all Trance Objects",
    response = classOf[TranceObject],
    responseContainer = "List"
  )
  def getAll=Action.async {
    tranceObjectRepository.getAll.map{ tranceObject =>
      Ok(Json.toJson(tranceObject))
    }
  }

  @ApiOperation(
    value = "Get a Trance Object",
    response = classOf[TranceObject]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Trance Object not found")
  ))
  def getTranceObject(@ApiParam(value="The id of the Trance Object to fetch") id: UUID) =
    Action.async{ _ =>
      tranceObjectRepository.getSingleItem(id).map{ maybeTranceObject =>
        maybeTranceObject.map { tranceObject =>
          Ok(Json.toJson(tranceObject))
        }.getOrElse(NotFound)
      }}

  @ApiOperation(
    value = "Add a new Trance Object to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Trance Object format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Trance Object in Json Format", required = true, dataType = "models.Query", paramType = "body")
  ))
  def createTranceObject() = Action.async(parse.json) {
    println("JSON PARSES!!!!!")
//        tranceObjectRepository.addEntity(TranceObject.apply(_id = None, name = "Test me", abr = None, column = []))
    _.body.validate[TranceObject].map { tranceObject =>
      tranceObjectRepository.addEntity(tranceObject).map{ _ =>
        Created
      }
    }.getOrElse(Future.successful(BadRequest("Invalid Todo format")))
  }

  @ApiOperation(
    value = "Update a Trance Object",
    response = classOf[TranceObject]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Trance Object format")
  )
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The updated Trance Object, in Json Format", required = true, dataType = "models.TranceObject", paramType = "body")
  )
  )
  def updateTranceObject(@ApiParam(value = "The id of the Trance Object to update")id: UUID) =
    Action.async(parse.json) {
      req => req.body.validate[TranceObject].map { tranceObject =>
        tranceObjectRepository.updateEntity(id, tranceObject).map {
          case Some(tranceObject) => Ok(Json.toJson(tranceObject))
          case _ => NotFound
        }
      }.getOrElse(Future.successful(BadRequest("Invalid Json")))
    }

  @ApiOperation(
    value = "Delete a Trance Object",
    response = classOf[TranceObject]
  )
  def delete(@ApiParam(value = "The id of the Trance Object to delete") id: UUID) = Action.async { req =>
    tranceObjectRepository.deleteEntity(id).map {
      case Some(tranceObject) => Ok(Json.toJson(tranceObject))
      case _ => NotFound
    }
  }

}
