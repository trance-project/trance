package shredding.examples.tpch

import shredding.wmcc.RecordValue

object TestData {
  //nation
  val nation = List(
    RecordValue("n_nationkey" -> 1, "n_name"->"Country1"),
    RecordValue("n_nationkey" -> 2, "n_name"->"Country2"),
    RecordValue("n_nationkey" -> 3, "n_name"->"Country3")

  )

  //partsupp
  val part = List(
    RecordValue("p_partkey" -> 1, "p_name" -> "part 1"),
    RecordValue("p_partkey" -> 2, "p_name" -> "part 2"),
    RecordValue("p_partkey" -> 3, "p_name" -> "part 3"),
    RecordValue("p_partkey" -> 4, "p_name" -> "part 4"),
    RecordValue("p_partkey" -> 5, "p_name" -> "part 5"),
  )
  val partsupp = List(
    RecordValue("ps_partkey" -> 1, "ps_suppkey" -> 1),
    RecordValue("ps_partkey" -> 2, "ps_suppkey" -> 1),
    RecordValue("ps_partkey" -> 3, "ps_suppkey" -> 2),
    RecordValue("ps_partkey" -> 4, "ps_suppkey" -> 3),
    RecordValue("ps_partkey" -> 5, "ps_suppkey" -> 3),
    RecordValue("ps_partkey" -> 1, "ps_suppkey" -> 4)
  )
  //supplier
  val supplier = List(
    RecordValue("s_suppkey" -> 1, "s_name" -> "supplier A", "s_nationkey" -> 1),
    RecordValue("s_suppkey" -> 2, "s_name" -> "supplier B", "s_nationkey" -> 1),
    RecordValue("s_suppkey" -> 3, "s_name" -> "supplier C", "s_nationkey" -> 2),
    RecordValue("s_suppkey" -> 4, "s_name" -> "supplier D", "s_nationkey" -> 3)
  )
  //lineitem
  val lineitem = List(
    RecordValue("l_partkey" -> 1, "l_orderkey"->1, "l_suppkey"->1,"l_quantity"->1),
    RecordValue("l_partkey" -> 1, "l_orderkey"->2, "l_suppkey"->1,"l_quantity"->1),
    RecordValue("l_partkey" -> 2, "l_orderkey"->1, "l_suppkey"->1,"l_quantity"->1),
    RecordValue("l_partkey" -> 2, "l_orderkey"->3, "l_suppkey"->2,"l_quantity"->1),
    RecordValue("l_partkey" -> 3, "l_orderkey"->1, "l_suppkey"->2,"l_quantity"->1),
    RecordValue("l_partkey" -> 4, "l_orderkey"->5, "l_suppkey"->3,"l_quantity"->1),
    RecordValue("l_partkey" -> 5, "l_orderkey"->4, "l_suppkey"->3,"l_quantity"->1),
  )
  //orders
  val orders = List(
    RecordValue("o_orderkey"->1,"o_custkey"->1, "o_orderdate" -> "2018"),
    RecordValue("o_orderkey"->2,"o_custkey"->1, "o_orderdate" -> "2018"),
    RecordValue("o_orderkey"->3,"o_custkey"->2, "o_orderdate" -> "2019"),
    RecordValue("o_orderkey"->4,"o_custkey"->3, "o_orderdate" -> "2019"),
    RecordValue("o_orderkey"->5,"o_custkey"->2, "o_orderdate" -> "2019")
  )
  //customers
  val customers = List(
    RecordValue("c_custkey" -> 1, "c_name" -> "Test Customer1", "c_nationkey" ->  1 ),
    RecordValue("c_custkey" -> 2, "c_name" -> "Test Customer2", "c_nationkey" ->  1 ),
    RecordValue("c_custkey" -> 3, "c_name" -> "Test Customer3", "c_nationkey" ->  1 ),
    RecordValue("c_custkey" -> 4, "c_name" -> "Test Customer4", "c_nationkey" ->  2 ),
    RecordValue("c_custkey" -> 5, "c_name" -> "Test Customer5", "c_nationkey" ->  3 ),
    RecordValue("c_custkey" -> 6, "c_name" -> "Test Customer6", "c_nationkey" ->  3 )
  )
}
