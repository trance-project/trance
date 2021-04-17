package v1.query

import javax.inject.Inject

import play.api.routing.Router.Routes
import play.api.routing.SimpleRouter
import play.api.routing.sird._

/**
  * Routes and URLs to the QueryResource controller.
  */
class QueryRouter @Inject()(controller: QueryController) extends SimpleRouter {
  val prefix = "/query"

  def link(id: QueryId): String = {
    import io.lemonlabs.uri.dsl._
    val url = prefix / id.toString
    url.toString()
  }

  /**
    * Examines the url and extract data to pass 
    * along to the controller
    */
  override def routes: Routes = {

    // simple test: 
    // http --verbose GET http://localhost:9000/v1/query/list
    case GET(p"/list") =>
      // this gets a single query by id
      // function getQuery with no args should return all queries
      controller.getQueries

    // simple test: 
    // http --verbose GET http://localhost:9000/v1/query/1
    case GET(p"/$id") =>
      // this gets a single query by id
      // function getQuery(id) just gets whatever 
      // query is associated to the specified id
      controller.getQuery(id) 

    // simple test:
    //http --verbose POST http://localhost:9000/v1/query/1 title="updateme" body="jsondata"
    case POST(p"/$id") =>
      // this gets a single query by id
      // function updateQuery just gets whatever 
      // query is associated to the specified id
      controller.updateQuery(id) 

    // simple test: 
    // http --verbose DELTE http://localhost:9000/v1/query/1
    case DELETE(p"/$id") =>
      // this deletes a single query by id
      // function deleteQuery is responsible for this
      controller.deleteQuery(id)    

    // http --verbose GET http://localhost:9000/v1/query/1
    case GET(p"/$id/shredded") =>
      // this just returns the query, 
      // but it should actually call out to the compiler
      controller.getQuery(id)

    case r => 
      sys.error(s"Route not found: $r")
      

  }

}
