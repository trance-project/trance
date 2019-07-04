
package experiments
/** Generated code **/
import shredding.examples.tpch._
    case class Record1341(p_name: String, l_qty: Double)
case class Record1344(o_orderdate: String, o_parts: List[Record1341])
case class Query1Out(c_name: String, c_orders: List[Record1344])
object Query1 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    
val C = TPCHLoader.loadCustomer[Customer].toList
val O = TPCHLoader.loadOrders[Orders].toList
val L = TPCHLoader.loadLineitem[Lineitem].toList
val P = TPCHLoader.loadPart[Part].toList
    var end0 = System.currentTimeMillis() - start0
    def f(){
      val x1288 = C
val x1290 = O
val x1295 = { val hm1336 = x1288.groupBy{ case x1291 => {val x1293 = x1291.c_custkey
x1293} }
x1290.flatMap(x1292 => hm1336.get({val x1294 = x1292.o_custkey
x1294}) match {
 case Some(a) => a.map(v => (v, x1292))
 case _ => Nil
}) }
val x1297 = L
val x1303 = { val hm1337 = x1295.groupBy{ case (x1298, x1299) => {val x1301 = x1299.o_orderkey
x1301} }
x1297.flatMap(x1300 => hm1337.get({val x1302 = x1300.l_orderkey
x1302}) match {
 case Some(a) => a.map(v => (v, x1300))
 case _ => Nil
}) }
val x1305 = P
val x1312 = { val hm1338 = x1303.groupBy{ case ((x1306, x1307), x1308) => {val x1310 = x1308.l_partkey
x1310} }
x1305.flatMap(x1309 => hm1338.get({val x1311 = x1309.p_partkey
x1311}) match {
 case Some(a) => a.map(v => (v, x1309))
 case _ => Nil
}) }
val x1322 = { val grps1339 = x1312.groupBy{ case (((x1313, x1314), x1315), x1316) => { val x1317 = (x1313,x1314)
x1317 }}
 grps1339.map(x1321 => (x1321._1, x1321._2.map{case (((x1313, x1314), x1315), x1316) => val x1318 = x1316.p_name
val x1319 = x1315.l_quantity
val x1320 = Record1341(x1318, x1319)
x1320})).toList }
val x1330 = { val grps1342 = x1322.groupBy{ case ((x1323, x1324), x1325) => { val x1326 = (x1323)
x1326 }}
 grps1342.map(x1329 => (x1329._1, x1329._2.map{case ((x1323, x1324), x1325) => val x1327 = x1324.o_orderdate
val x1328 = Record1344(x1327, x1325)
x1328})).toList }
val x1335 = x1330.map{ case (x1331, x1332) => { 
  val x1333 = x1331.c_name
  val x1334 = Query1Out(x1333, x1332)
  x1334 }}
x1335
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
