package framework.plans

import framework.common._
import scala.collection.mutable.Map

class CachePlanner(covers: Map[Integer, CostEstimate], capacity: Double = 1.0) {

	val knapsack = Map.empty[Integer, CExpr]

	def solve(availability: Double = 0.0): Unit = {

		var newAvail = availability

		while (newAvail < capacity){
			val (sig, candidate) = covers.head

			val sizeWithAdd = candidate.est.outSize + newAvail

			if (sizeWithAdd <= capacity) {
				// add to knapsack
				knapsack(sig) = candidate.plan
				// updated capacity
				newAvail = sizeWithAdd
			}

			solve(newAvail)
		
		}
		
	}


}