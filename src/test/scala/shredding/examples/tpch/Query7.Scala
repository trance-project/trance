
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record2943(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record2949(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record3010(p_name: String, uniqueId: Long) extends CaseClassRecord
case class Record3011(n_name: String, part_names: List[Record3010], uniqueId: Long) extends CaseClassRecord
case class Query3Out(p_name: String, suppliers: List[Record2943], customers: List[Record2949], uniqueId: Long) extends CaseClassRecord
object Query7 {
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
val PS = TPCHLoader.loadPartSupp[PartSupp].toList
val S = TPCHLoader.loadSupplier[Supplier].toList
    val Query7 = { val x2868 = P 
val x2870 = PS 
val x2875 = { val hm2939 = x2868.groupBy{ case x2871 => {val x2873 = x2871.p_partkey 
x2873 } }
x2870.flatMap(x2872 => hm2939.get({val x2874 = x2872.ps_partkey 
x2874 }) match {
 case Some(a) => a.map(v => (v, x2872))
 case _ => Nil
}) } 
val x2877 = S 
val x2883 = { val hm2940 = x2875.groupBy{ case (x2878, x2879) => {val x2881 = x2879.ps_suppkey 
x2881 } }
x2877.flatMap(x2880 => hm2940.get({val x2882 = x2880.s_suppkey 
x2882 }) match {
 case Some(a) => a.map(v => (v, x2880))
 case _ => Nil
}) } 
val x2893 = { val grps2941 = x2883.groupBy{ case ((x2884, x2885), x2886) => { val x2887 = (x2884) 
x2887  } }
 grps2941.toList.map(x2891 => (x2891._1, x2891._2.flatMap{ 
   case ((x2884, x2885), null) =>  Nil
   case ((x2884, x2885), x2886) => {val x2892 = (x2885,x2886) 
x2892 } match {
   case (null,_) => Nil
   case (x2885,x2886) => List({val x2888 = x2886.s_name 
val x2889 = x2886.s_nationkey 
val x2890 = Record2943(x2888, x2889, newId) 
x2890   })
 }
} ) ) } 
val x2895 = L 
val x2901 = { val hm2944 = x2893.groupBy{ case (x2896, x2897) => {val x2899 = x2896.p_partkey 
x2899 } }
x2895.flatMap(x2898 => hm2944.get({val x2900 = x2898.l_partkey 
x2900 }) match {
 case Some(a) => a.map(v => (v, x2898))
 case _ => Nil
}) } 
val x2903 = O 
val x2910 = { val hm2945 = x2901.groupBy{ case ((x2904, x2905), x2906) => {val x2908 = x2906.l_orderkey 
x2908 } }
x2903.flatMap(x2907 => hm2945.get({val x2909 = x2907.o_orderkey 
x2909 }) match {
 case Some(a) => a.map(v => (v, x2907))
 case _ => Nil
}) } 
val x2912 = C 
val x2920 = { val hm2946 = x2910.groupBy{ case (((x2913, x2914), x2915), x2916) => {val x2918 = x2916.o_custkey 
x2918 } }
x2912.flatMap(x2917 => hm2946.get({val x2919 = x2917.c_custkey 
x2919 }) match {
 case Some(a) => a.map(v => (v, x2917))
 case _ => Nil
}) } 
val x2932 = { val grps2947 = x2920.groupBy{ case ((((x2921, x2922), x2923), x2924), x2925) => { val x2926 = (x2921,x2922) 
x2926  } }
 grps2947.toList.map(x2930 => (x2930._1, x2930._2.flatMap{ 
   case ((((x2921, x2922), x2923), x2924), null) =>  Nil
   case ((((x2921, x2922), x2923), x2924), x2925) => {val x2931 = (x2923,x2924,x2925) 
x2931 } match {
   case (null,null,_) => Nil
   case (x2923,x2924,x2925) => List({val x2927 = x2925.c_name 
val x2928 = x2925.c_nationkey 
val x2929 = Record2949(x2927, x2928, newId) 
x2929   })
 }
} ) ) } 
val x2938 = x2932.map{ case ((x2933, x2934), x2935) => { 
  val x2936 = x2933.p_name 
  val x2937 = Query3Out(x2936, x2934, x2935, newId) 
  x2937 }} 
x2938               }
    var end0 = System.currentTimeMillis() - start0
    val N = TPCHLoader.loadNation[Nation].toList
    def f(){
      val x2957 = N 
val x2959 = Query7 
val x2962 = x2959.flatMap{ x2961 => 
 x2957.map{ x2960 => (x2960, x2961) }
} 
val x2967 = x2962.flatMap{ case (x2963, x2964) => 
  val x2965 = x2964.suppliers 
  x2965.flatMap(x2966 => {
    List(((x2963, x2964), x2966))
})} 
val x2976 = x2967.flatMap{ case ((x2968, x2969), x2970) => 
  val x2971 = x2969.customers 
  x2971.flatMap(x2972 => {
  if({val x2973 = x2972.c_nationkey 
  val x2974 = x2968.n_nationkey 
  val x2975 = x2973 == x2974 
  x2975   }) {  List((((x2968, x2969), x2970), x2972))} else {  List((((x2968, x2969), x2970), null))}
})} 
val x2984 = { val grps3006 = x2976.groupBy{ case (((x2977, x2978), x2979), x2980) => { val x2981 = (x2977,x2978,x2979) 
x2981  } }
 grps3006.toList.map(x2982 => (x2982._1, x2982._2.foldLeft(0){ 
 case (acc3007, (((x2977, x2978), x2979), x2980)) => {val x2983 = (x2980) 
x2983 } match {
   case (null) => acc3007
   case _ => acc3007 + {1}
 }
} ) ) } 
val x2999 = { val grps3008 = x2984.groupBy{ case ((x2985, x2986, x2987), x2988) => { val x2989 = (x2985) 
x2989  } }
 grps3008.toList.map(x2992 => (x2992._1, x2992._2.flatMap{ 
   case ((x2985, x2986, x2987), 0) =>  Nil
   case ((x2985, x2986, x2987), x2988) => {val x2998 = (x2986,x2987) 
x2998 } match {
   case (null,_) => Nil
   case (x2986,x2987) => List({val x2990 = x2986.p_name 
val x2991 = Record3010(x2990, newId) 
x2991  })
 }
} ) ) } 
val x3004 = x2999.map{ case (x3000, x3001) => { 
  val x3002 = x3000.n_name 
  val x3003 = Record3011(x3002, x3001, newId) 
  x3003 }} 
x3004        
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
