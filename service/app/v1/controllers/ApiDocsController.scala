package v1.controllers

import play.api._
import play.api.mvc._

import javax.inject.Inject

class ApiDocsController @Inject()(cc: ControllerComponents, configuration: Configuration) extends AbstractController(cc){
//  def redirectToDocs = Action {
//    val basePath = configuration.underlying.getString("swagger.api.uri")
//    Redirect(
//      url= "/assets/lib/swagger-ui/index.html",
//      queryStringParams = Map("url" -> Seq(s"$basePath/swagger.json"))
//    )
//  }

  def home = Action { req =>
    PermanentRedirect("/lib/swagger-ui/index.html?url=/assets/swagger.json")
    //    val basePath = configuration.underlying.getString("swagger.api.uri")
    //    Redirect(
    //      url= "/assets/lib/swagger-ui/index.html",
    //      queryStringParams = Map("url" -> Seq(s"$basePath/swagger.json"))
    //    )
  }
}
