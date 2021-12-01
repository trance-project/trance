package v1.query
import javax.inject.{Inject, Provider}

import play.api.MarkerContext

import scala.concurrent.{ExecutionContext, Future}
import play.api.libs.json._

/**
  * DTO for displaying post information.
  */
case class QueryResource(id: String, link: String, title: String, body: String)

object QueryResource {
  /**
    * Mapping to read/write a QueryResource out as a JSON value.
    */
    implicit val format: Format[QueryResource] = Json.format
}


/**
  * Controls access to the backend data, returning [[QueryResource]]
  */
class QueryResourceHandler @Inject()(
    routerProvider: Provider[QueryRouter],
    queryRepository: QueryRepository)(implicit ec: ExecutionContext) {

  def create(queryInput: QueryFormInput)(
      implicit mc: MarkerContext): Future[QueryResource] = {
    val data = QueryData(QueryId("999"), queryInput.title, queryInput.body)
    // We don't actually create the query, so return what we have
    queryRepository.create(data).map { id =>
      createQueryResource(data)
    }
  }

  def compile(queryInput: QueryFormInput)(
      implicit mc: MarkerContext): Future[QueryResource] = {
    val data = QueryData(QueryId("999"), queryInput.title, queryInput.body)
    // We don't actually create the query, so return what we have
    queryRepository.compile(data).map { program =>
      createQueryResource(QueryData(data.id, data.title, program.toString()))
    }
  }

  def lookup(id: String)(
      implicit mc: MarkerContext): Future[Option[QueryResource]] = {
    val queryFuture = queryRepository.get(QueryId(id))
    queryFuture.map { maybeQueryData =>
      maybeQueryData.map { queryData =>
        createQueryResource(queryData)
      }
    }
  }

  def find(implicit mc: MarkerContext): Future[Iterable[QueryResource]] = {
    queryRepository.list().map { queryDataList =>
      queryDataList.map(queryData => createQueryResource(queryData))
    }
  }

  private def createQueryResource(p: QueryData): QueryResource = {
    QueryResource(p.id.toString, routerProvider.get.link(p.id), p.title, p.body)
  }

}
