package sprkloader

import org.apache.spark.Partitioner

class SkewPartitioner(override val numPartitions: Int) extends Partitioner {

  def defaultPartitionAssignment(key:Any): Int = {
	val hcmod = key.hashCode % numPartitions
	hcmod + (if (hcmod < 0) numPartitions else 0)
  }

  override def getPartition(key: Any): Int = {
	val k = key.asInstanceOf[(Any, Int)]
	if (k._2 != -1) k._2
	  else defaultPartitionAssignment(key)
  }

  override def equals(that: Any): Boolean = {
	that match {
	  case sp:SkewPartitioner => sp.numPartitions == numPartitions
	  case _ => false
	}
  }

}
