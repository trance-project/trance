
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record1530(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1533(o_orderdate: String, o_parts: List[Record1530], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record1533], uniqueId: Long) extends CaseClassRecord
object Query1 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x1475 = C 
val x1477 = O 
val x1482 = { val hm1525 = x1475.groupBy{ case x1478 => {val x1480 = x1478.c_custkey 
x1480 } }
x1477.flatMap(x1479 => hm1525.get({val x1481 = x1479.o_custkey 
x1481 }) match {
 case Some(a) => a.map(v => (v, x1479))
 case _ => Nil
}) } 
val x1484 = L 
val x1490 = { val hm1526 = x1482.groupBy{ case (x1485, x1486) => {val x1488 = x1486.o_orderkey 
x1488 } }
x1484.flatMap(x1487 => hm1526.get({val x1489 = x1487.l_orderkey 
x1489 }) match {
 case Some(a) => a.map(v => (v, x1487))
 case _ => Nil
}) } 
val x1492 = P 
val x1499 = { val hm1527 = x1490.groupBy{ case ((x1493, x1494), x1495) => {val x1497 = x1495.l_partkey 
x1497 } }
x1492.flatMap(x1496 => hm1527.get({val x1498 = x1496.p_partkey 
x1498 }) match {
 case Some(a) => a.map(v => (v, x1496))
 case _ => Nil
}) } 
val x1510 = { val grps1528 = x1499.groupBy{ case (((x1500, x1501), x1502), x1503) => { val x1504 = (x1500,x1501) 
x1504  } }
 grps1528.toList.map(x1508 => (x1508._1, x1508._2.flatMap{ 
   case (((x1500, x1501), x1502), null) =>  Nil
   case (((x1500, x1501), x1502), x1503) => {val x1509 = (x1502,x1503) 
x1509 } match {
   case (null,_) => Nil
   case (x1502,x1503) => List({val x1505 = x1503.p_name 
val x1506 = x1502.l_quantity 
val x1507 = Record1530(x1505, x1506, newId) 
x1507   })
 }
} ) ) } 
val x1519 = { val grps1531 = x1510.groupBy{ case ((x1511, x1512), x1513) => { val x1514 = (x1511) 
x1514  } }
 grps1531.toList.map(x1517 => (x1517._1, x1517._2.flatMap{ 
   case ((x1511, x1512), null) =>  Nil
   case ((x1511, x1512), x1513) => {val x1518 = (x1512,x1513) 
x1518 } match {
   case (null,_) => Nil
   case (x1512,x1513) => List({val x1515 = x1512.o_orderdate 
val x1516 = Record1533(x1515, x1513, newId) 
x1516  })
 }
} ) ) } 
val x1524 = x1519.map{ case (x1520, x1521) => { 
  val x1522 = x1520.c_name 
  val x1523 = Query1Out(x1522, x1521, newId) 
  x1523 }} 
x1524          
    }
    var time = List[Long]()
    for (i <- 1 to 5) {
      var start = System.currentTimeMillis()
      f
      var end = System.currentTimeMillis() - start
      time = time :+ end
    }
    val avg = (time.sum/5)
    println(end0+","+avg)
 }
}
