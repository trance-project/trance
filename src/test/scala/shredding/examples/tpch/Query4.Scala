
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record1349(p_name: String, l_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Record1352(o_orderdate: String, o_parts: List[Record1349], uniqueId: Long) extends CaseClassRecord
case class Record1398(c_name: String, p_name: String, month: String, t_qty: Double, uniqueId: Long) extends CaseClassRecord
case class Query1Out(c_name: String, c_orders: List[Record1352], uniqueId: Long) extends CaseClassRecord
object Query4 {
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
    val Query4 = { val x1294 = C 
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
x1343           }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1359 = Query4 
val x1363 = x1359.flatMap{ case x1360 => 
  val x1361 = x1360.c_orders 
  x1361.flatMap(x1362 => {
    List((x1360, x1362))
})} 
val x1368 = x1363.flatMap{ case (x1364, x1365) => 
  val x1366 = x1365.o_parts 
  x1366.flatMap(x1367 => {
    List(((x1364, x1365), x1367))
})} 
val x1377 = x1368.flatMap{ case ((x1369, x1370), x1371) => 
  val x1372 = x1370.o_parts 
  x1372.flatMap(x1373 => {
  if({val x1374 = x1373.p_name 
  val x1375 = x1371.p_name 
  val x1376 = x1374 == x1375 
  x1376   }) {  List((((x1369, x1370), x1371), x1373))} else {  List((((x1369, x1370), x1371), None))}
})} 
val x1386 = { val grps1396 = x1377.groupBy{ case (((x1378, x1379), x1380), x1381) => { val x1382 = (x1378,x1379,x1380) 
x1382  } }
 grps1396.toList.map(x1384 => (x1384._1, x1384._2.foldLeft(0){ 
 case (acc1397, (((x1378, x1379), x1380), x1381)) => {val x1385 = (x1381) 
x1385 } match {
   case (null) => acc1397
   case _ => acc1397 + {val x1383 = x1381.l_qty 
x1383 }
 }
} ) ) } 
val x1395 = x1386.map{ case ((x1387, x1388, x1389), x1390) => { 
  val x1391 = x1387.c_name 
  val x1392 = x1389.p_name 
  val x1393 = x1388.o_orderdate 
  val x1394 = Record1398(x1391, x1392, x1393, x1390, newId) 
  x1394 }} 
x1395      
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
