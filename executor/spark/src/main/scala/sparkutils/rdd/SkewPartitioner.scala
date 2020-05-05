package sparkutils.rdd

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast

class Balancer(override val numPartitions: Int, hkeys: Broadcast[Set[Any]]) extends Partitioner {

  def defaultPartitionAssignment(key:Any): Int = {
	  val hcmod = key.hashCode % numPartitions
	  hcmod + (if (hcmod < 0) numPartitions else 0)
  }

  override def getPartition(key: Any): Int = {
	  val k = key.asInstanceOf[(Any, Int)]
    if (hkeys.value(k._1)) k._2
    else defaultPartitionAssignment(key)
  }

  override def equals(that: Any): Boolean = {
	  that match {
	    case b:Balancer => b.numPartitions == numPartitions
	    case _ => false
	  }
  }

}

class SkewPartitioner(override val numPartitions: Int) extends Partitioner {

  def defaultPartitionAssignment(key:Any): Int = {
	  val hcmod = key.hashCode % numPartitions
	  hcmod + (if (hcmod < 0) numPartitions else 0)
  }

  override def getPartition(key: Any): Int = {
    try{
      val k = key.asInstanceOf[(Any, Int)]
	    if (k._2 != -1) k._2
	    else defaultPartitionAssignment(key)
    }catch{
      case _:Throwable => defaultPartitionAssignment(key)
    }
  }

  override def equals(that: Any): Boolean = {
	  that match {
	    case sp:SkewPartitioner => sp.numPartitions == numPartitions
	    case _ => false
	  }
  }

}
