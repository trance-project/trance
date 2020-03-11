package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.LinearizedNRC

object Test0 extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c2__Dc_orders_1", s"o2__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(l, relL,
    Singleton(Tuple("l_partkey" -> lr("l_partkey"), "l_qty" -> lr("l_quantity"))))

}
 
object Test1 extends TPCHBase {

  val name = "Test1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c2__Dc_orders_1", s"o2__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(l, relL,
    ForeachUnion(p, relP, IfThenElse(
      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))

}

object Test2 extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c2__Dc_orders_1", s"o2__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(l, relL,
    ForeachUnion(p, relP, IfThenElse(
      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))

}

object Test3 extends TPCHBase {

  val name = "Test2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c2__Dc_orders_1", s"o2__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = 
  ForeachUnion(o, relO,
    Singleton(Tuple("orderdate" -> or("o_orderdate"), "o_parts" ->
      GroupBy(
        ForeachUnion(l, relL,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
            ForeachUnion(p, relP, IfThenElse(
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))),
        List("p_name"),
        List("l_qty"),
        DoubleType
        ))))

}