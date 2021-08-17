package framework.optimize

import org.scalatest.FunSuite
import framework.common._
import framework.examples.tpch._
import framework.examples.genomic._
import framework.nrc._
import framework.plans.{Equals => CEquals, Project => CProject}
import scala.collection.mutable.HashMap
import framework.plans._

class TestStatsCollector extends TestBase {

  val zep = false

  // test("standard comilation route"){

  //   val seBuilder = SEBuilder(progs)
  //   seBuilder.updateSubexprs()

  //   val subs = seBuilder.sharedSubs()

  //   val ces = CEBuilder.buildCoverMap(subs)
    
  //   val statsCollector = new StatsCollector(progs)
  //   val stats = if (zep){
  //     statsCollector.getCost(subs, ces)
  //   }else Map.empty[String, Statistics]

  //   println(stats)
  //   println("this is the column map")
  //   println(statsCollector.colMap)
  // }

  test("shred comilation route"){

    val seBuilder = SEBuilder(sprogs)
    seBuilder.updateSubexprs()

    val subs = seBuilder.sharedSubs()
    // printSE(subs)

    val ces = CEBuilder.buildCoverMap(subs)
    
    val statsCollector = new StatsCollector(sprogs)
    val stats = if (zep){
      statsCollector.getCost(subs, ces)
    }else Map.empty[String, Statistics]


  }

}