package v1

import javax.inject._
import play.api._
import play.api.mvc._

import javax.inject.Inject

class ApiDocsController @Inject()(cc: ControllerComponents, configuration: Configuration) extends AbstractController(cc){
  def redirectToDocs = Action {
    val basePath = configuration.underlying.getString("swagger.api.uri")
    Redirect(
      url= "/assets/lib/swagger-ui/index.html",
      queryStringParams = Map("url" -> Seq(s"$basePath/swagger.json"))
    )
  }
}
