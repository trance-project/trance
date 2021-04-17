package v1.query

import javax.inject.Inject

import play.api.Logger
import play.api.data.Form
import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

case class QueryFormInput(title: String, body: String)

/**
  * Takes HTTP requests and produces JSON.
  */
class QueryController @Inject()(cc: QueryControllerComponents)(
    implicit ec: ExecutionContext)
    extends QueryBaseController(cc) {

  private val logger = Logger(getClass)

  private val form: Form[QueryFormInput] = {
    import play.api.data.Forms._

    Form(
      mapping(
        "title" -> nonEmptyText,
        "body" -> text
      )(QueryFormInput.apply)(QueryFormInput.unapply)
    )
  }

  def getQueries: Action[AnyContent] = QueryAction.async { implicit request =>
    logger.trace("index: ")
    queryResourceHandler.find.map { queries =>
      Ok(Json.toJson(queries))
    }
  }

  def updateQuery(id: String): Action[AnyContent] = QueryAction.async { implicit request =>
    logger.trace("update query: id = $id")
    // this just processes the submitted json, 
    // but it should update the entry in mongo 
    // with the appropriate values in the posted json
    processJsonQuery()
  }

  def getQuery(id: String): Action[AnyContent] = QueryAction.async {
    implicit request =>
      logger.trace(s"get query: id = $id")
      // this just calls to the stub repo, but this should 
      // query mongodb and return the information associated 
      // to the query id
      queryResourceHandler.lookup(id).map { query =>
        Ok(Json.toJson(query))
      }

  }
  

  def deleteQuery(id: String): Action[AnyContent] = QueryAction.async {
    implicit request =>
      logger.trace(s"query deleted: id = $id")
      // this just calls to the stub repo, but this should 
      // remove the query from mongodb
      queryResourceHandler.lookup(id).map { query =>
        Ok(Json.toJson(query))
      }
  }

  private def processJsonQuery[A]()(
      implicit request: QueryRequest[A]): Future[Result] = {
    def failure(badForm: Form[QueryFormInput]) = {
      Future.successful(BadRequest(badForm.errorsAsJson))
    }

    def success(input: QueryFormInput) = {
      queryResourceHandler.create(input).map { query =>
        Created(Json.toJson(query)).withHeaders(LOCATION -> query.link)
      }
    }

    form.bindFromRequest().fold(failure, success)
  }
}
