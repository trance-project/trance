package framework.plans

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{HashMap, Map}

case class Estimate(inSize: Double, outSize: Double, 
  inRows: Double, outRows: Double, cpu: Double, network: Double){

  def total: Double = inSize + outSize + inRows + outRows + cpu + network

}

case class CostSE(wid: Int, subplan: CExpr, height: Int, est: Estimate)
case class CostCE(cover: CExpr, sig: Integer, ses: List[CostSE], est: Estimate)
case class CostEstimate(plan: CExpr, profit: Double, est: Estimate)

class Cost(stats: Map[String, Statistics]) {

  val DISKREAD = 0.1
  val NETWORK = 10.0
  val NOSHUFF = 0.00001
  val RAMREAD = 0.001
  val RAMWRITE = 0.1
  val SELECTIVITY = 0.33
  val INDEXCOST = 1.1

  val NESTSIZE = 2.0
  val NESTROWS = 10.0

  val default = Estimate(1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

  // estimate
  def selectCovers(covers: IMap[Integer, CNamed], subs: Map[Integer, List[SE]]): Map[Integer, CostEstimate] = {

    val selected = Map.empty[Integer, CostEstimate]

    covers.foreach{ c =>
      val ses = subs(c._1)
      // total cost of evaluating all subexpressions
      val totalwork = ses.map(s => estimate(s.subplan).total).reduce(_ + _) 

      // total cost of cover + add cache + (access cache * accesses)
      val cest = estimate(c._2.e)
      val covercost = cest.total + estMaterialization(cest.outSize) + 
        (estRetrieval(cest.outSize) * ses.size)

      val profit = totalwork - covercost

      if (profit > 0) selected(c._1) = CostEstimate(c._2, profit, cest)

    }

    selected

  }

  def printEstimateAndStat(covers: IMap[Integer, CNamed], subs: Map[Integer, List[SE]]): Unit = {
    covers.foreach{ c =>
      val plan = c._2.e
      val est = estimate(plan)
      println("Cover:")
      println(Printer.quote(plan))
      println(stats.getOrElse(plan.vstr, StatsCollector.default))
      println(est)
      println("Subs:")
      val ses = subs(c._1)
      ses.foreach{ s =>
        val est = estimate(s.subplan)
        println(Printer.quote(s.subplan))
        println(stats.getOrElse(s.subplan.vstr, StatsCollector.default))
        println(est)
      }
      println("")
    }
  }

  def estMaterialization(card: Double): Double = card * RAMWRITE

  def estRetrieval(card: Double): Double = card * RAMREAD


  def compare(s1: (CExpr, Statistics), s2: (CExpr, Statistics)): CExpr = 
    if (s1._2.lessThan(s2._2)) s1._1 else s2._1

  // TODO
  def estSelectivity(plan: CExpr): Double = plan match {
    case _ => SELECTIVITY
  }

  // TODO this will need histogram information
  def estNestColumn(plan: CExpr, attr: String): (Double, Double) = (NESTSIZE, NESTROWS)

  // single plan estimate
  def estimate(plan: CExpr): Estimate = {
    val stat = stats.getOrElse(plan.vstr, StatsCollector.default)
    val sel = estSelectivity(plan)
    plan match {

      // for testing
      case y if stats.isEmpty => default

      // todo inner vs outer join
      // this is the simpliest approach right now, some combination of left and right
      // need to use cardinality information where possible
      case j:JoinOp =>
        val leftEst = estimate(j.left)
        val rightEst = estimate(j.right)

        // network cost of the largest relation, plus what it costs to 
        // perform the operation give estimated output rows
        val network = leftEst.network + rightEst.network + (leftEst.outRows * NETWORK) + (stat.rowCount * .00002)
        val cpu = leftEst.cpu + rightEst.cpu + (stat.rowCount * .00002)

        val insize = leftEst.outSize + rightEst.outSize
        val inrows = leftEst.outRows * rightEst.outRows

        // some factor of cardinalities
        //val outsize = leftEst.outSize + rightEst.outSize 
        val outsize = (stat.sizeInBytes / 1024)
        val outrows = stat.rowCount

        Estimate(insize, outsize, inrows, outrows, network, cpu)

      // same here with unnest, need to use average nested collection sizes
      // cpu time is more because we are grouping, network is quite a 
      // bit since potentially large collections are being shuffled
      case n:Nest => 
        val childEst = estimate(n.in)
        // some growth factor based on column, for now just nestsize
        val outsize = childEst.outSize * NESTSIZE
        val outrows = childEst.outRows * ((n.key.size * 1.0) / n.in.tp.attrs.size)

        // four times a simple operation (noshuff)
        val cpu = (childEst.outRows * (.00004)) + childEst.cpu
        // network could be dependent on the size of the values
        val network = (childEst.outRows * NETWORK) + childEst.network

        Estimate(childEst.inSize, outsize, childEst.inRows, outrows, cpu, network)

      // TODO see what spark does for aggregation estimate
      // reduce factor based on key and values compared to child columns
      // effects outsize and outrows
      // cpu cost should be evaluated, but seems to be slightly more than 
      // a simple operation; TODO what other factors should be considered?
      case r:Reduce => 
        val childEst = estimate(r.in)
        val factor = ((r.keys.size + r.values.size) * 1.0) / r.in.tp.attrs.size

        val outsize = childEst.outSize * factor
        // possibly use column stats (dedup on keys)
        val outrows = childEst.outRows * factor

        // two times a simple operation (noshuff)
        val cpu = (childEst.outRows * (.00002)) + childEst.cpu
        val network = (childEst.outRows * NETWORK) + childEst.network

        Estimate(childEst.inSize, outsize, childEst.inRows, outrows, cpu, network)

      // note statistics from spark seem to not be estimated for 
      // outerunnest, make sure that this gets around that
      // TODO capture column statistics to get proper NESTSIZE and NESTROWS
      // outsize should be the child outsize * some average size of nested collections
      // outrows should be the child outrows * some average rows in the nested collections
      // minor cpu addition, but no network cost
      case u:UnnestOp => 
        val childEst = estimate(u.in)
        val cpu = (childEst.outRows * NOSHUFF) + childEst.cpu
        val outsize = childEst.outSize * NESTSIZE
        val outrows = childEst.outRows * NESTROWS
        Estimate(childEst.inSize, outsize, childEst.inRows, outrows, cpu, childEst.network)

      // TODO selectivity estmate, using default currently
      // outsize and rows are based on the selectivity of the filter
      // only minor cpu additions, no network cost 
      case s:Select => 
        val childEst = estimate(s.in)
        val cpu = (childEst.outRows * NOSHUFF) + childEst.cpu
        val (outsize, outrows) = s.p match {
          case Constant(true) => (childEst.outSize, childEst.outRows)
          case _ => (childEst.outSize * sel, childEst.outRows * sel)
        }
        Estimate(childEst.inSize, outsize, childEst.inRows, outrows, cpu, childEst.network)

      // projection will reduce the number of columns, so should 
      // reduce the size of the child output by a factor proportional 
      // to the number of columns being removed
      // cpu addition is minor, but no changes to rows or network cost
      case p:Projection => 
        val childEst = estimate(p.in)
        val outsize = ((p.tp.attrs.size * 1.0) / p.in.tp.attrs.size) * childEst.outSize
        val cpu = childEst.cpu + (childEst.outRows * NOSHUFF)

        Estimate(childEst.inSize, outsize, childEst.inRows, outsize, cpu, childEst.network)

      // adding an index will add one column, so minor addition to size
      // adding the index will also take a small amount of cpu time
      // no network or additional rows added
      case i:AddIndex => 
        val childEst = estimate(i.in)
        val size = childEst.outSize * INDEXCOST
        val cpu = childEst.cpu + (childEst.outRows * NOSHUFF)
        Estimate(childEst.inSize, size, childEst.inRows, childEst.outRows, cpu, childEst.network)

      // the base cost estimate for all input relations
      // size and rows directly from stats
      // assume no network cost
      // cpu is just the time to scan
      case _ => 
        val size = (stat.sizeInBytes / 1024) + 0.0
        val rows = stat.rowCount + 0.0
        Estimate(size, size, rows, rows, stat.rowCount * DISKREAD, 0.0)

    }
  }


}