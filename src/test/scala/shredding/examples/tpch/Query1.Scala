
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
    case class Record1349(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1352(o_orderdate: String, o_parts: List[Record1349], uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record1352], uniqueId: Long) extends CaseClassRecord
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
      val x1294 = C 
val x1296 = O 
val x1301 = { val hm1344 = x1294.groupBy{ case x1297 => {val x1299 = x1297.c_custkey 
x1299 } }
x1296.flatMap(x1298 => hm1344.get({val x1300 = x1298.o_custkey 
x1300 }) match {
 case Some(a) => a.map(v => (v, x1298))
 case _ => Nil
}) } 
val x1303 = L 
val x1309 = { val hm1345 = x1301.groupBy{ case (x1304, x1305) => {val x1307 = x1305.o_orderkey 
x1307 } }
x1303.flatMap(x1306 => hm1345.get({val x1308 = x1306.l_orderkey 
x1308 }) match {
 case Some(a) => a.map(v => (v, x1306))
 case _ => Nil
}) } 
val x1311 = P 
val x1318 = { val hm1346 = x1309.groupBy{ case ((x1312, x1313), x1314) => {val x1316 = x1314.l_partkey 
x1316 } }
x1311.flatMap(x1315 => hm1346.get({val x1317 = x1315.p_partkey 
x1317 }) match {
 case Some(a) => a.map(v => (v, x1315))
 case _ => Nil
}) } 
val x1329 = { val grps1347 = x1318.groupBy{ case (((x1319, x1320), x1321), x1322) => { val x1323 = (x1319,x1320) 
x1323  } }
 grps1347.toList.map(x1327 => (x1327._1, x1327._2.flatMap{ 
   case (((x1319, x1320), x1321), null) =>  Nil
   case (((x1319, x1320), x1321), x1322) => {val x1328 = (x1321,x1322) 
x1328 } match {
   case (null,_) => Nil
   case _ => List({val x1324 = x1322.p_name 
val x1325 = x1321.l_quantity 
val x1326 = Record1349(x1324, x1325, newId) 
x1326   })
 }
} ) ) } 
val x1338 = { val grps1350 = x1329.groupBy{ case ((x1330, x1331), x1332) => { val x1333 = (x1330) 
x1333  } }
 grps1350.toList.map(x1336 => (x1336._1, x1336._2.flatMap{ 
   case ((x1330, x1331), null) =>  Nil
   case ((x1330, x1331), x1332) => {val x1337 = (x1331,x1332) 
x1337 } match {
   case (null,_) => Nil
   case _ => List({val x1334 = x1331.o_orderdate 
val x1335 = Record1352(x1334, x1332, newId) 
x1335  })
 }
} ) ) } 
val x1343 = x1338.map{ case (x1339, x1340) => { 
  val x1341 = x1339.c_name 
  val x1342 = Query1Out(x1341, x1340, newId) 
  x1342 }} 
x1343          
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
