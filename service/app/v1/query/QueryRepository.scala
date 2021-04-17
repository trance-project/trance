package v1.query

import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import play.api.libs.concurrent.CustomExecutionContext
import play.api.{Logger, MarkerContext}

import scala.concurrent.Future

final case class QueryData(id: QueryId, title: String, body: String)

class QueryId private (val underlying: Int) extends AnyVal {
  override def toString: String = underlying.toString
}

object QueryId {
  def apply(raw: String): QueryId = {
    require(raw != null)
    new QueryId(Integer.parseInt(raw))
  }
}

class QueryExecutionContext @Inject()(actorSystem: ActorSystem)
    extends CustomExecutionContext(actorSystem, "repository.dispatcher")

/**
  * A pure non-blocking interface for the QueryRepository.
  */
trait QueryRepository {
  def create(data: QueryData)(implicit mc: MarkerContext): Future[QueryId]

  def list()(implicit mc: MarkerContext): Future[Iterable[QueryData]]

  def get(id: QueryId)(implicit mc: MarkerContext): Future[Option[QueryData]]
}

/**
  * A trivial implementation for the Query Repository.
  *
  * A custom execution context is used here to establish that blocking operations should be
  * executed in a different thread than Play's ExecutionContext, which is used for CPU bound tasks
  * such as rendering.
  */
@Singleton
class QueryRepositoryImpl @Inject()()(implicit ec: QueryExecutionContext)
    extends QueryRepository {

  private val logger = Logger(this.getClass)

  private val queryList = List(
    QueryData(QueryId("1"), "query1", "for x in R union {(x)}"),
    QueryData(QueryId("2"), "query2", "for y in blew"),
    QueryData(QueryId("3"), "query3", "for z in beep"),
    QueryData(QueryId("4"), "query4", "for a in boop"),
    QueryData(QueryId("5"), "query5", "for b in baap")
  )

  override def list()(
      implicit mc: MarkerContext): Future[Iterable[QueryData]] = {
    Future {
      logger.trace(s"list: ")
      queryList
    }
  }

  override def get(id: QueryId)(
      implicit mc: MarkerContext): Future[Option[QueryData]] = {
    Future {
      logger.trace(s"get: id = $id")
      queryList.find(query => query.id == id)
    }
  }

  def create(data: QueryData)(implicit mc: MarkerContext): Future[QueryId] = {
    Future {
      logger.trace(s"create: data = $data")
      data.id
    }
  }

}
