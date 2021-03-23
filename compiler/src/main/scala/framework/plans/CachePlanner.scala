package framework.plans

import framework.common._
import scala.collection.mutable.Map

class CachePlanner(covers: Map[Integer, CostEstimate], capacity: Double = 1.0) {

	val knapsack = Map.empty[Integer, CExpr]

	val sortedCovers = covers.toVector.sortBy(s => s._2.profit)

	def solve(availability: Double = 0.0, i: Int = 0): Unit = {

		var newAvail = availability

		if (availability < capacity){
			
			val (sig, candidate) = sortedCovers(i)
			val sizeWithAdd = candidate.est.outSize + newAvail

			if (sizeWithAdd <= capacity) {

				knapsack(sig) = candidate.plan
				newAvail = sizeWithAdd

			}
			
			solve(newAvail, i+1)

		}

		
	}


}