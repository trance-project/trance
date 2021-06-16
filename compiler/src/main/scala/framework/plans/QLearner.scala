package framework.plans

/**
 * Based on https://cb372.github.io/rl-in-scala/
 * Pole balancing problem
 */

import scala.util.Random
// import scala.collection.mutable.Map

case class QLearner(lr: Double, gamma: Double, epsilon: Double, q: Map[CacheState, Map[CacheAction, Double]])

case class ActionResult(reward: Double, nextState: CacheState)

trait AgentBehavior[AgentData]{
	def chooseAction(agentData: AgentData, state: CacheState): //, validActions: List[Action]):
		(CacheAction, ActionResult => AgentData)
}

object QLearner{

	implicit def agentBehavior: AgentBehavior[QLearner] = 
		new AgentBehavior[QLearner]{

			def chooseAction(agentData: QLearner, state: CacheState): //, validActions: List[Action]):
				(CacheAction, ActionResult => QLearner) = {

					// initialize with profits
					val validActions = state.candidatePool.map(x => CacheAction(x._1, x._2) -> 0.0).toMap

					val actionValues = agentData.q.getOrElse(state, validActions)

					val (chosenAction, currentActionValue) = epsilonGreedy(actionValues, agentData.epsilon)

					val updateStateActionValue: ActionResult => QLearner = {
						actionResult => 

							val validActions = state.candidatePool.map(x => CacheAction(x._1, x._2) -> 0.0).toMap

							val nextStateActionValues = 
								agentData.q.getOrElse(actionResult.nextState, validActions)

							val maxNextStateActionValue = 
								nextStateActionValues.values.fold(Double.MinValue)(_ max _)

							// lr is how often you accept the old value verse the new value
							// reward is just the profit
							// gamma is the discount factor, balances future rewards
							val updatedActionValue = 
								currentActionValue + agentData.lr * (actionResult.reward + agentData.gamma * maxNextStateActionValue - currentActionValue)

							val updatedActionValues = actionValues + (chosenAction -> updatedActionValue)
							val updatedQ = agentData.q + (state -> updatedActionValues)

							agentData.copy(q = updatedQ)

					}

					(chosenAction, updateStateActionValue)

				}

				private def epsilonGreedy(actionValues: Map[CacheAction, Double], epsilon: Double): (CacheAction, Double) = {
					if (Random.nextDouble() < epsilon) {
						Random.shuffle(actionValues.toList).head
					}else{
			          val sorted   = actionValues.toList.sortBy(_._2).reverse
			          val maxValue = sorted.head._2
			          Random.shuffle(sorted.takeWhile(_._2 == maxValue)).head
					}
				}

		}

}