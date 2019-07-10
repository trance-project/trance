
package experiments
/** Generated code **/
import shredding.examples.tpch._
case class Record1337(p_name: String, l_qty: Double)
case class Record1340(o_orderdate: String, o_parts: List[Record1337])
case class Record1385(c_name: String, p_name: String, month: String, t_qty: Double)
case class Query1Out(c_name: String, c_orders: List[Record1340])
object Query4 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    val Query4 = { val x1284 = C 
val x1286 = O 
val x1291 = { val hm1332 = x1284.groupBy{ case x1287 => {val x1289 = x1287.c_custkey 
x1289 } }
x1286.flatMap(x1288 => hm1332.get({val x1290 = x1288.o_custkey 
x1290 }) match {
 case Some(a) => a.map(v => (v, x1288))
 case _ => Nil
}) } 
val x1293 = L 
val x1299 = { val hm1333 = x1291.groupBy{ case (x1294, x1295) => {val x1297 = x1295.o_orderkey 
x1297 } }
x1293.flatMap(x1296 => hm1333.get({val x1298 = x1296.l_orderkey 
x1298 }) match {
 case Some(a) => a.map(v => (v, x1296))
 case _ => Nil
}) } 
val x1301 = P 
val x1308 = { val hm1334 = x1299.groupBy{ case ((x1302, x1303), x1304) => {val x1306 = x1304.l_partkey 
x1306 } }
x1301.flatMap(x1305 => hm1334.get({val x1307 = x1305.p_partkey 
x1307 }) match {
 case Some(a) => a.map(v => (v, x1305))
 case _ => Nil
}) } 
val x1318 = { val grps1335 = x1308.groupBy{ case (((x1309, x1310), x1311), x1312) => { val x1313 = (x1309,x1310) 
x1313  }}
 grps1335.toList.map(x1317 => (x1317._1, x1317._2.map{ case (((x1309, x1310), x1311), x1312) => 
 { val x1314 = x1312.p_name 
val x1315 = x1311.l_quantity 
val x1316 = Record1337(x1314, x1315) 
x1316    } } ) ) } 
val x1326 = { val grps1338 = x1318.groupBy{ case ((x1319, x1320), x1321) => { val x1322 = (x1319) 
x1322  }}
 grps1338.toList.map(x1325 => (x1325._1, x1325._2.map{ case ((x1319, x1320), x1321) => 
 { val x1323 = x1320.o_orderdate 
val x1324 = Record1340(x1323, x1321) 
x1324   } } ) ) } 
val x1331 = x1326.map{ case (x1327, x1328) => { 
  val x1329 = x1327.c_name 
  val x1330 = Query1Out(x1329, x1328) 
  x1330 }} 
x1331           }
    var end0 = System.currentTimeMillis() - start0
    
    def f(){
      val x1347 = Query4 
val x1351 = x1347.flatMap{ case x1348 => 
  val x1349 = x1348.c_orders 
  x1349.flatMap(x1350 => {
    List((x1348, x1350))
})} 
val x1356 = x1351.flatMap{ case (x1352, x1353) => 
  val x1354 = x1353.o_parts 
  x1354.flatMap(x1355 => {
    List(((x1352, x1353), x1355))
})} 
val x1365 = x1356.flatMap{ case ((x1357, x1358), x1359) => 
  val x1360 = x1358.o_parts 
  x1360.flatMap(x1361 => {
  if({val x1362 = x1361.p_name 
  val x1363 = x1359.p_name 
  val x1364 = x1362 == x1363 
  x1364   }) {  List((((x1357, x1358), x1359), x1361))} else {  List((((x1357, x1358), x1359), None))}
})} 
val x1373 = { val grps1383 = x1365.groupBy{ case (((x1366, x1367), x1368), x1369) => { val x1370 = (x1366,x1367,x1368) 
x1370  }}
 grps1383.toList.map(x1372 => (x1372._1, x1372._2.foldLeft(0){ case (acc1384, (((x1366, x1367), x1368), x1369)) => 
 if ((((x1366, x1367), x1368), x1369)._2.equals(None)) { 0 } else { acc1384 + {val x1371 = x1369.l_qty 
x1371 } } } ) ) } 
val x1382 = x1373.map{ case ((x1374, x1375, x1376), x1377) => { 
  val x1378 = x1374.c_name 
  val x1379 = x1376.p_name 
  val x1380 = x1375.o_orderdate 
  val x1381 = Record1385(x1378, x1379, x1380, x1377) 
  x1381 }} 
x1382      
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
