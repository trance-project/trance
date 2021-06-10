package framework.plans

/**
 * Based on https://cb372.github.io/rl-in-scala/
 * Pole balancing problem
 */

import java.lang.Math._
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.Map

case class CacheAction(sig: Integer, cand: CostEstimate)

case class CacheState(candidatePool: Map[Integer, CostEstimate], 
	selected: Map[Integer, CNamed], size: Double, profit: Double)

trait Environment{

	def possibleActions(currentState: CacheState): List[CacheAction]
	def step(currentState: CacheState, actionTaken: CacheAction): (CacheState, Double)
	def isTerminal(state: CacheState): Boolean

}

object CacheSelectionProblem{ 

	implicit val environment: Environment = 
		new Environment {

			override def possibleActions(currentState: CacheState): List[CacheAction] = 
				currentState.candidatePool.toList.map{ case (s,c) => CacheAction(s, c) }

			override def step(currentState: CacheState, actionTaken: CacheAction): (CacheState, Double) = {
				
				val curr_weight = actionTaken.cand.est.outSize
				val curr_profit = actionTaken.cand.profit

				val acc_size = currentState.size - curr_weight
				val acc_profit = currentState.profit + curr_profit

				val nextState = 
					if ((acc_size - curr_weight) < 0.0) currentState
					else {
						val updatedPool = currentState.candidatePool - actionTaken.sig
						CacheState(updatedPool, currentState.selected + (actionTaken.sig -> actionTaken.cand.plan), acc_size, acc_profit)
					}

				val reward = if (isTerminal(nextState)) -1.0 else 0.0

				(nextState, reward)

			}

			override def isTerminal(state: CacheState): Boolean = {
				state.size == 0.0 || state.candidatePool.isEmpty
			}

		}

}

class CacheQLearner(candidates: Map[Integer, CostEstimate], capacity: Double) {

	import CacheSelectionProblem._

	private val initialState = CacheState(candidates, Map.empty[Integer, CNamed], capacity, 0.0)
	private val initialAgentData = QLearner(lr = 0.1, gamma = 1.0, epsilon = 0.1, q = IMap.empty[CacheState, IMap[CacheAction,Double]])

	private val env: Environment = implicitly
	private val agentBehavior: AgentBehavior[QLearner] = implicitly

	var agentData = initialAgentData
	var cacheState = initialState
	var timeElapsed = 0.0
	var maxTimeElapsed = 0.0
	var episodeCount = 1

	def step(): Unit = {

		timeElapsed += 0.02

		val currentState = cacheState
		val (nextAction, updateAgent) = agentBehavior.chooseAction(agentData, currentState)
		val (nextState, reward) = env.step(cacheState, nextAction)

		agentData = updateAgent(ActionResult(reward, nextState))
		cacheState = nextState

	}

	def endOfEpisode(): Unit = {

		maxTimeElapsed = maxTimeElapsed max timeElapsed 
		timeElapsed = 0.0
		episodeCount += 1
		cacheState = initialState

	}

	def run(): Map[Integer, CNamed] = {
		var start = System.currentTimeMillis()
		while(!env.isTerminal(cacheState)) step()
		val currState = cacheState
		endOfEpisode()
		var end = System.currentTimeMillis() - start
		println(s"QLearning took: $end ms")
		currState.selected
	}
}

