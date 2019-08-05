
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record2338(p__F: Part, S__F: Int, PS__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2339(p__F: Part, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2340(p_name: String, suppliers: Record2338, customers: Record2339, uniqueId: Long) extends CaseClassRecord
case class Record2347(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2356(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2436(Query5__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record2437(lbl: Record2436, uniqueId: Long) extends CaseClassRecord
case class Input_Query5__DFlat2438(p_name: String, suppliers: Int, customers: Int, uniqueId: Long) extends CaseClassRecord
case class Input_Query5__DDict2438(suppliers: (List[(Int, List[Record2347])], Unit), customers: (List[(Int, List[Record2356])], Unit), uniqueId: Long) extends CaseClassRecord
case class Record2451(p_name: String, cnt: Int, uniqueId: Long) extends CaseClassRecord
case class Record2452(_1: Record2436, _2: List[Record2451], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record2339, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q3Flat, _2: List[Record2340], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record2339, _2: List[Record2356], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record2338, _2: List[Record2347], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record2338, uniqueId: Long) extends CaseClassRecord
object ShredQuery5 {
 def main(args: Array[String]){
    var start0 = System.currentTimeMillis()
    var id = 0L
    def newId: Long = {
      val prevId = id
      id += 1
      prevId
    }
    
val C__F = 1
val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
val O__F = 2
val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
val L__F = 3
val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
val P__F = 4
val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())
val PS__F = 5
val PS__D = (List((PS__F, TPCHLoader.loadPartSupp[PartSupp].toList)), ())
val S__F = 6
val S__D = (List((S__F, TPCHLoader.loadSupplier[Supplier].toList)), ())
    val ShredQuery5 = { val x2176 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x2177 = RecM_ctx1(x2176, newId) 
val x2178 = List(x2177) 
val M_ctx1 = x2178
val x2179 = M_ctx1
val x2181 = M_ctx1 
val x2182 = P__D._1 
val x2184 = x2182 
val x2189 = { val hm2335 = x2181.groupBy{case x2185 => { val x2187 = x2185.lbl 
val x2188 = x2187.P__F 
x2188   } }
 x2184.flatMap{x2186 => hm2335.get(x2186._1) match {
 case Some(a) => a.map(a1 => (a1, x2186._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x2205 = { val grps2336 = x2189.groupBy{ case (x2190, x2191) => { val x2192 = (x2190) 
x2192  } }
 grps2336.toList.map(x2203 => (x2203._1, x2203._2.flatMap{ 
   case (x2190, null) =>  Nil
   case (x2190, x2191) => {val x2204 = (x2191) 
x2204 } match {
   case (null) => Nil
   case (x2191) => List({val x2193 = x2191.p_name 
val x2194 = x2190.lbl 
val x2195 = x2194.S__F 
val x2196 = x2194.PS__F 
val x2197 = Record2338(x2191, x2195, x2196, newId) 
val x2198 = x2194.C__F 
val x2199 = x2194.L__F 
val x2200 = x2194.O__F 
val x2201 = Record2339(x2191, x2198, x2199, x2200, newId) 
val x2202 = Record2340(x2193, x2197, x2201, newId) 
x2202          })
 }
} ) ) } 
val x2210 = x2205.map{ case (x2206, x2207) => { 
  val x2208 = x2206.lbl 
  val x2209 = RecM_flat1(x2208, x2207, newId) 
  x2209 }} 
val M_flat1 = x2210
val x2211 = M_flat1
val x2213 = M_flat1 
val x2217 = x2213.flatMap{ case x2214 => 
  val x2215 = x2214._2 
  x2215.flatMap(x2216 => {
    List((x2214, x2216))
})} 
val x2222 = x2217.map{ case (x2218, x2219) => { 
  val x2220 = x2219.suppliers 
  val x2221 = RecM_ctx2(x2220, newId) 
  x2221 }} 
val x2223 = x2222.distinct 
val M_ctx2 = x2223
val x2224 = M_ctx2
val x2226 = M_ctx2 
val x2227 = PS__D._1 
val x2229 = x2227 
val x2238 = { val hm2342 = x2226.groupBy{case x2230 => { val x2232 = x2230.lbl 
val x2233 = x2232.PS__F 
x2233   } }
 val join1 = x2229.flatMap{x2231 => hm2342.get(x2231._1) match {
 case Some(a) => x2231._2
 case _ => Nil
 }}
 val join2 = x2226.groupBy{case x2230 => { val x2235 = x2230.lbl 
val x2236 = x2235.p__F 
val x2237 = x2236.p_partkey 
x2237    } }
 join1.flatMap(x2231 => join2.get({ val x2234 = x2231.ps_partkey 
x2234  }) match {
   case Some(a) => a.map(a1 => (a1, x2231))
   case _ => Nil
 })
} 
val x2239 = S__D._1 
val x2241 = x2239 
val x2249 = { val hm2344 = x2238.groupBy{case (x2242, x2243) => { val x2245 = x2242.lbl 
val x2246 = x2245.S__F 
x2246   } }
 val join1 = x2241.flatMap{x2244 => hm2344.get(x2244._1) match {
 case Some(a) => x2244._2
 case _ => Nil
 }}
 val join2 = x2238.groupBy{case (x2242, x2243) => { val x2248 = x2243.ps_suppkey 
x2248  } }
 join1.flatMap(x2244 => join2.get({ val x2247 = x2244.s_suppkey 
x2247  }) match {
   case Some(a) => a.map(a1 => (a1, x2244))
   case _ => Nil
 })
} 
val x2259 = { val grps2345 = x2249.groupBy{ case ((x2250, x2251), x2252) => { val x2253 = (x2250) 
x2253  } }
 grps2345.toList.map(x2257 => (x2257._1, x2257._2.flatMap{ 
   case ((x2250, x2251), null) =>  Nil
   case ((x2250, x2251), x2252) => {val x2258 = (x2251,x2252) 
x2258 } match {
   case (null,_) => Nil
   case (x2251,x2252) => List({val x2254 = x2252.s_name 
val x2255 = x2252.s_nationkey 
val x2256 = Record2347(x2254, x2255, newId) 
x2256   })
 }
} ) ) } 
val x2264 = x2259.map{ case (x2260, x2261) => { 
  val x2262 = x2260.lbl 
  val x2263 = RecM_flat2(x2262, x2261, newId) 
  x2263 }} 
val M_flat2 = x2264
val x2265 = M_flat2
val x2267 = M_flat1 
val x2271 = x2267.flatMap{ case x2268 => 
  val x2269 = x2268._2 
  x2269.flatMap(x2270 => {
    List((x2268, x2270))
})} 
val x2276 = x2271.map{ case (x2272, x2273) => { 
  val x2274 = x2273.customers 
  val x2275 = RecM_ctx3(x2274, newId) 
  x2275 }} 
val x2277 = x2276.distinct 
val M_ctx3 = x2277
val x2278 = M_ctx3
val x2280 = M_ctx3 
val x2281 = L__D._1 
val x2283 = x2281 
val x2292 = { val hm2349 = x2280.groupBy{case x2284 => { val x2286 = x2284.lbl 
val x2287 = x2286.L__F 
x2287   } }
 val join1 = x2283.flatMap{x2285 => hm2349.get(x2285._1) match {
 case Some(a) => x2285._2
 case _ => Nil
 }}
 val join2 = x2280.groupBy{case x2284 => { val x2289 = x2284.lbl 
val x2290 = x2289.p__F 
val x2291 = x2290.p_partkey 
x2291    } }
 join1.flatMap(x2285 => join2.get({ val x2288 = x2285.l_partkey 
x2288  }) match {
   case Some(a) => a.map(a1 => (a1, x2285))
   case _ => Nil
 })
} 
val x2293 = O__D._1 
val x2295 = x2293 
val x2303 = { val hm2351 = x2292.groupBy{case (x2296, x2297) => { val x2299 = x2296.lbl 
val x2300 = x2299.O__F 
x2300   } }
 val join1 = x2295.flatMap{x2298 => hm2351.get(x2298._1) match {
 case Some(a) => x2298._2
 case _ => Nil
 }}
 val join2 = x2292.groupBy{case (x2296, x2297) => { val x2302 = x2297.l_orderkey 
x2302  } }
 join1.flatMap(x2298 => join2.get({ val x2301 = x2298.o_orderkey 
x2301  }) match {
   case Some(a) => a.map(a1 => (a1, x2298))
   case _ => Nil
 })
} 
val x2304 = C__D._1 
val x2306 = x2304 
val x2315 = { val hm2353 = x2303.groupBy{case ((x2307, x2308), x2309) => { val x2311 = x2307.lbl 
val x2312 = x2311.C__F 
x2312   } }
 val join1 = x2306.flatMap{x2310 => hm2353.get(x2310._1) match {
 case Some(a) => x2310._2
 case _ => Nil
 }}
 val join2 = x2303.groupBy{case ((x2307, x2308), x2309) => { val x2314 = x2309.o_custkey 
x2314  } }
 join1.flatMap(x2310 => join2.get({ val x2313 = x2310.c_custkey 
x2313  }) match {
   case Some(a) => a.map(a1 => (a1, x2310))
   case _ => Nil
 })
} 
val x2326 = { val grps2354 = x2315.groupBy{ case (((x2316, x2317), x2318), x2319) => { val x2320 = (x2316) 
x2320  } }
 grps2354.toList.map(x2324 => (x2324._1, x2324._2.flatMap{ 
   case (((x2316, x2317), x2318), null) =>  Nil
   case (((x2316, x2317), x2318), x2319) => {val x2325 = (x2317,x2318,x2319) 
x2325 } match {
   case (null,null,_) => Nil
   case (x2317,x2318,x2319) => List({val x2321 = x2319.c_name 
val x2322 = x2319.c_nationkey 
val x2323 = Record2356(x2321, x2322, newId) 
x2323   })
 }
} ) ) } 
val x2331 = x2326.map{ case (x2327, x2328) => { 
  val x2329 = x2327.lbl 
  val x2330 = RecM_flat3(x2329, x2328, newId) 
  x2330 }} 
val M_flat3 = x2331
val x2332 = M_flat3
val x2333 = (x2179,x2211,x2224,x2265,x2278,x2332) 
x2333                                        }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
val Query5__F = ShredQuery5._1.head.lbl
val Query5__D = (ShredQuery5._2, Input_Q3_Dict1((ShredQuery5._4, Unit), (ShredQuery5._6, Unit)))
    def f(){
      val x2367 = Record2436(Query5__F, newId) 
val x2368 = Record2437(x2367, newId) 
val x2369 = List(x2368) 
val M_ctx1 = x2369
val x2370 = M_ctx1
val x2372 = M_ctx1 
val x2373 = Query5__D._1 
val x2375 = x2373 
val x2380 = { val hm2441 = x2372.groupBy{case x2376 => { val x2378 = x2376.lbl 
val x2379 = x2378.Query5__F 
x2379   } }
 x2375.flatMap{x2377 => hm2441.get(x2377._1) match {
 case Some(a) => a.map(a1 => (a1, x2377._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x2381 = Query5__D._2 
val x2382 = x2381.customers 
val x2383 = x2382._1 
val x2385 = x2383 
val x2390 = { val hm2443 = x2385.map{ case x2388 => (x2388._1, x2388._2)}.toMap
x2380.flatMap{ 
  case (x2386, null) =>  List(((x2386, null), null))
  case (x2386, x2387) => hm2443.get({val x2389 = x2387.customers 
x2389 }) match {
   case Some(a) => a.flatMap{ case x2388 => 
     if ({true} == {true }) { List(((x2386, x2387), x2388)) } else { List(((x2386, x2387), null)) }
   }
   case _ => List(((x2386, x2387), null))
 }
}} 
val x2391 = x2381.suppliers 
val x2392 = x2391._1 
val x2394 = x2392 
val x2402 = { val hm2444 = x2390.groupBy{case ((x2395, x2396), x2397) => { val x2399 = x2396.suppliers 
x2399  } }
 val join1 = x2394.flatMap{x2398 => hm2444.get(x2398._1) match {
 case Some(a) => x2398._2
 case _ => Nil
 }}
 val join2 = x2390.groupBy{case ((x2395, x2396), x2397) => { val x2401 = x2397.c_nationkey 
x2401  } }
 join1.flatMap(x2398 => join2.get({ val x2400 = x2398.s_nationkey 
x2400  }) match {
   case Some(a) => a.map(a1 => (a1, x2398))
   case _ => Nil
 })
} 
val x2410 = { val grps2445 = x2402.groupBy{ case (((x2403, x2404), x2405), x2406) => { val x2407 = (x2403,x2404,x2405) 
x2407  } }
 grps2445.toList.map(x2408 => (x2408._1, x2408._2.foldLeft(0){ 
 case (acc2446, (((x2403, x2404), x2405), x2406)) => {val x2409 = (x2406) 
x2409 } match {
   case (null) => acc2446
   case _ => acc2446 + {1}
 }
} ) ) } 
val x2419 = { val grps2447 = x2410.groupBy{ case ((x2411, x2412, x2413), x2414) => { val x2415 = (x2411,x2412) 
x2415  } }
 grps2447.toList.map(x2416 => (x2416._1, x2416._2.foldLeft(0){ 
 case (acc2448, ((x2411, x2412, x2413), x2414)) => {val x2418 = (x2413) 
x2418 } match {
   case (null) => acc2448
   case _ => acc2448 + {1}
 }
} ) ) } 
val x2428 = { val grps2449 = x2419.groupBy{ case ((x2420, x2421), x2422) => { val x2423 = (x2420) 
x2423  } }
 grps2449.toList.map(x2426 => (x2426._1, x2426._2.flatMap{ 
   case ((x2420, x2421), 0) =>  Nil
   case ((x2420, x2421), x2422) => {val x2427 = (x2421,x2422) 
x2427 } match {
   case (null,_) => Nil
   case (x2421,x2422) => List({val x2424 = x2421.p_name 
val x2425 = Record2451(x2424, x2422, newId) 
x2425  })
 }
} ) ) } 
val x2433 = x2428.map{ case (x2429, x2430) => { 
  val x2431 = x2429.lbl 
  val x2432 = Record2452(x2431, x2430, newId) 
  x2432 }} 
val M_flat1 = x2433
val x2434 = M_flat1
val x2435 = (x2370,x2434) 
x2435                     
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
