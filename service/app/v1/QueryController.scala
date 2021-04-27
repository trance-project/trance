package v1

import io.swagger.annotations.{Api, ApiImplicitParam, ApiImplicitParams, ApiOperation, ApiParam, ApiResponse, ApiResponses}
import models.{Query, QueryRepository, TranceObject}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Api(value = "/query")
class QueryController @Inject()(
                               cc: ControllerComponents,
                               queryRepository: QueryRepository
                               ) extends AbstractController (cc){

  @ApiOperation(
    value = "Find all Querys",
    response = classOf[Query],
    responseContainer = "List"
  )
  def getAll=Action.async {
    queryRepository.getAll.map{ query =>
      Ok(Json.toJson(query))
    }
  }

  @ApiOperation(
    value = "Get a Query",
    response = classOf[TranceObject]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Query not found")
  ))
  def getQuery(@ApiParam(value="The id of the Query to fetch") id: UUID) =
    Action.async{ req =>
      queryRepository.getSingleItem(id).map{ maybeQuery =>
        maybeQuery.map { query =>
          Ok(Json.toJson(query))
        }.getOrElse(NotFound)
      }}

  @ApiOperation(
    value = "Add a Query to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Query in Json Format", required = true, dataType = "models.Query", paramType = "body")
  ))
  def createQuery() = Action.async(parse.json) {
    println("JSON PARSES!!!!!")
    queryRepository.addEntity(Query.apply(_id = None, title = "Test me", body = "Some empty String"))
    _.body.validate[Query].map { query =>
      queryRepository.addEntity(query).map{ _ =>
        Created
      }
    }.getOrElse(Future.successful(BadRequest("Invalid Todo format")))
  }

  @ApiOperation(
    value = "Update a Query",
    response = classOf[Query]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  )
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The updated Query, in Json Format", required = true, dataType = "models.Query", paramType = "body")
  )
  )
  def updateUpdateQuery(@ApiParam(value = "The id of the Query to update")id: UUID) =
    Action.async(parse.json) {
      req => req.body.validate[Query].map { query =>
        queryRepository.updateEntity(id, query).map {
          case Some(query) => Ok(Json.toJson(query))
          case _ => NotFound
        }
      }.getOrElse(Future.successful(BadRequest("Invalid Json")))
    }

  @ApiOperation(
    value = "Delete a Query",
    response = classOf[Query]
  )
  def delete(@ApiParam(value = "The id of the Query to delete") id: UUID) = Action.async { req =>
    queryRepository.deleteEntity(id).map {
      case Some(query) => Ok(Json.toJson(query))
      case _ => NotFound
    }
  }

}
