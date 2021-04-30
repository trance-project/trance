package v1.controllers

import io.swagger.annotations._
import models.{Query, TranceObject}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}
import v1.repository.QueryRepository

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Api(value = "/nrccode")
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
  def createQuery() =
    Action.async(parse.json) {
    println("JSON PARSES!!!!!")
      val responseBody = "{\n    name: \"QuerySimple\",\n    key: \"For s in samples Union \",\n    labels: [{\n        name: \"sample\",\n        key: \"s.bcr_patient_uuid\"\n        }, {\n        name: \"mutations\",\n        key: \"For o in occurrences Union If (s.bcr_patient_uuid = o.donorId) Then \",\n        labels: [{\n            name: \"mutId\",\n            key: \"o.oid\",\n        }, {\n            name : \"scores\",\n            key : \"ReduceByKey[gene], [score], For t in o.transcript_consequences Union For c in copynumber Union If (t.gene_id = c.cn_gene_id AND c.cn_aliquot_uuid = s.bcr_aliquot_uuid) Then \",\n            labels : [{\n                name: \"gene\",\n                key : \"t.gene_id\"\n            }, {\n                name : \"score\",\n                key : \"((c.cn_copy_number + 0.01) * If (t.impact = HIGH) Then 0.8 Else If (t.impact = MODERATE) Then 0.5 Else If (t.impact = LOW) Then 0.3 Else 0.01   )\"\n            } ]\n        }]\n    }]\n}"
//    queryRepository.addEntity(Query.apply(_id = None, title = "Test me", body = "Some empty String"))
    _.body.validate[Query].map { query =>
      queryRepository.addEntity(query).map{ _ =>
        Created(responseBody)
      }
    }.getOrElse(Future.successful(BadRequest("Invalid nrc format")))
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
