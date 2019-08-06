
package experiments
/** Generated code **/
import shredding.core.CaseClassRecord
import shredding.examples.tpch._
case class Record3197(p__F: Part, S__F: Int, PS__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record3198(p__F: Part, C__F: Int, L__F: Int, O__F: Int, uniqueId: Long) extends CaseClassRecord
case class Record3199(p_name: String, suppliers: Record3197, customers: Record3198, uniqueId: Long) extends CaseClassRecord
case class Record3206(s_name: String, s_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record3215(c_name: String, c_nationkey: Int, uniqueId: Long) extends CaseClassRecord
case class Record3341(lbl: Q3Flat3, uniqueId: Long) extends CaseClassRecord
case class Record3346(Query7__F: Int, n__F: Nation, uniqueId: Long) extends CaseClassRecord
case class Record3347(n_name: String, part_names: Record3346, uniqueId: Long) extends CaseClassRecord
case class Record3348(_1: Q3Flat3, _2: List[Record3347], uniqueId: Long) extends CaseClassRecord
case class Record3349(lbl: Record3346, uniqueId: Long) extends CaseClassRecord
case class Input_Query7__DFlat3350(p_name: String, suppliers: Int, customers: Int, uniqueId: Long) extends CaseClassRecord
case class Input_Query7__DDict3350(suppliers: (List[(Int, List[Record3206])], Unit), customers: (List[(Int, List[Record3215])], Unit), uniqueId: Long) extends CaseClassRecord
case class Record3361(p_name: String, uniqueId: Long) extends CaseClassRecord
case class Record3362(_1: Record3346, _2: List[Record3361], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx1(lbl: Q3Flat, uniqueId: Long) extends CaseClassRecord
case class RecM_ctx3(lbl: Record3198, uniqueId: Long) extends CaseClassRecord
case class RecM_flat1(_1: Q3Flat, _2: List[Record3199], uniqueId: Long) extends CaseClassRecord
case class RecM_flat3(_1: Record3198, _2: List[Record3215], uniqueId: Long) extends CaseClassRecord
case class RecM_flat2(_1: Record3197, _2: List[Record3206], uniqueId: Long) extends CaseClassRecord
case class RecM_ctx2(lbl: Record3197, uniqueId: Long) extends CaseClassRecord
object ShredQuery7 {
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
    val ShredQuery7 = { val x3035 = Q3Flat(O__F, C__F, PS__F, S__F, L__F, P__F, newId) 
val x3036 = RecM_ctx1(x3035, newId) 
val x3037 = List(x3036) 
val M_ctx1 = x3037
val x3038 = M_ctx1
val x3040 = M_ctx1 
val x3041 = P__D._1 
val x3043 = x3041 
val x3048 = { val hm3194 = x3040.groupBy{case x3044 => { val x3046 = x3044.lbl 
val x3047 = x3046.P__F 
x3047   } }
 x3043.flatMap{x3045 => hm3194.get(x3045._1) match {
 case Some(a) => a.map(a1 => (a1, x3045._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x3064 = { val grps3195 = x3048.groupBy{ case (x3049, x3050) => { val x3051 = (x3049) 
x3051  } }
 grps3195.toList.map(x3062 => (x3062._1, x3062._2.flatMap{ 
   case (x3049, null) =>  Nil
   case (x3049, x3050) => {val x3063 = (x3050) 
x3063 } match {
   case (null) => Nil
   case (x3050) => List({val x3052 = x3050.p_name 
val x3053 = x3049.lbl 
val x3054 = x3053.S__F 
val x3055 = x3053.PS__F 
val x3056 = Record3197(x3050, x3054, x3055, newId) 
val x3057 = x3053.C__F 
val x3058 = x3053.L__F 
val x3059 = x3053.O__F 
val x3060 = Record3198(x3050, x3057, x3058, x3059, newId) 
val x3061 = Record3199(x3052, x3056, x3060, newId) 
x3061          })
 }
} ) ) } 
val x3069 = x3064.map{ case (x3065, x3066) => { 
  val x3067 = x3065.lbl 
  val x3068 = RecM_flat1(x3067, x3066, newId) 
  x3068 }} 
val M_flat1 = x3069
val x3070 = M_flat1
val x3072 = M_flat1 
val x3076 = x3072.flatMap{ case x3073 => 
  val x3074 = x3073._2 
  x3074.flatMap(x3075 => {
    List((x3073, x3075))
})} 
val x3081 = x3076.map{ case (x3077, x3078) => { 
  val x3079 = x3078.suppliers 
  val x3080 = RecM_ctx2(x3079, newId) 
  x3080 }} 
val x3082 = x3081.distinct 
val M_ctx2 = x3082
val x3083 = M_ctx2
val x3085 = M_ctx2 
val x3086 = PS__D._1 
val x3088 = x3086 
val x3097 = { val hm3201 = x3085.groupBy{case x3089 => { val x3091 = x3089.lbl 
val x3092 = x3091.PS__F 
x3092   } }
 val join1 = x3088.flatMap{x3090 => hm3201.get(x3090._1) match {
 case Some(a) => x3090._2
 case _ => Nil
 }}
 val join2 = x3085.groupBy{case x3089 => { val x3094 = x3089.lbl 
val x3095 = x3094.p__F 
val x3096 = x3095.p_partkey 
x3096    } }
 join1.flatMap(x3090 => join2.get({ val x3093 = x3090.ps_partkey 
x3093  }) match {
   case Some(a) => a.map(a1 => (a1, x3090))
   case _ => Nil
 })
} 
val x3098 = S__D._1 
val x3100 = x3098 
val x3108 = { val hm3203 = x3097.groupBy{case (x3101, x3102) => { val x3104 = x3101.lbl 
val x3105 = x3104.S__F 
x3105   } }
 val join1 = x3100.flatMap{x3103 => hm3203.get(x3103._1) match {
 case Some(a) => x3103._2
 case _ => Nil
 }}
 val join2 = x3097.groupBy{case (x3101, x3102) => { val x3107 = x3102.ps_suppkey 
x3107  } }
 join1.flatMap(x3103 => join2.get({ val x3106 = x3103.s_suppkey 
x3106  }) match {
   case Some(a) => a.map(a1 => (a1, x3103))
   case _ => Nil
 })
} 
val x3118 = { val grps3204 = x3108.groupBy{ case ((x3109, x3110), x3111) => { val x3112 = (x3109) 
x3112  } }
 grps3204.toList.map(x3116 => (x3116._1, x3116._2.flatMap{ 
   case ((x3109, x3110), null) =>  Nil
   case ((x3109, x3110), x3111) => {val x3117 = (x3110,x3111) 
x3117 } match {
   case (null,_) => Nil
   case (x3110,x3111) => List({val x3113 = x3111.s_name 
val x3114 = x3111.s_nationkey 
val x3115 = Record3206(x3113, x3114, newId) 
x3115   })
 }
} ) ) } 
val x3123 = x3118.map{ case (x3119, x3120) => { 
  val x3121 = x3119.lbl 
  val x3122 = RecM_flat2(x3121, x3120, newId) 
  x3122 }} 
val M_flat2 = x3123
val x3124 = M_flat2
val x3126 = M_flat1 
val x3130 = x3126.flatMap{ case x3127 => 
  val x3128 = x3127._2 
  x3128.flatMap(x3129 => {
    List((x3127, x3129))
})} 
val x3135 = x3130.map{ case (x3131, x3132) => { 
  val x3133 = x3132.customers 
  val x3134 = RecM_ctx3(x3133, newId) 
  x3134 }} 
val x3136 = x3135.distinct 
val M_ctx3 = x3136
val x3137 = M_ctx3
val x3139 = M_ctx3 
val x3140 = L__D._1 
val x3142 = x3140 
val x3151 = { val hm3208 = x3139.groupBy{case x3143 => { val x3145 = x3143.lbl 
val x3146 = x3145.L__F 
x3146   } }
 val join1 = x3142.flatMap{x3144 => hm3208.get(x3144._1) match {
 case Some(a) => x3144._2
 case _ => Nil
 }}
 val join2 = x3139.groupBy{case x3143 => { val x3148 = x3143.lbl 
val x3149 = x3148.p__F 
val x3150 = x3149.p_partkey 
x3150    } }
 join1.flatMap(x3144 => join2.get({ val x3147 = x3144.l_partkey 
x3147  }) match {
   case Some(a) => a.map(a1 => (a1, x3144))
   case _ => Nil
 })
} 
val x3152 = O__D._1 
val x3154 = x3152 
val x3162 = { val hm3210 = x3151.groupBy{case (x3155, x3156) => { val x3158 = x3155.lbl 
val x3159 = x3158.O__F 
x3159   } }
 val join1 = x3154.flatMap{x3157 => hm3210.get(x3157._1) match {
 case Some(a) => x3157._2
 case _ => Nil
 }}
 val join2 = x3151.groupBy{case (x3155, x3156) => { val x3161 = x3156.l_orderkey 
x3161  } }
 join1.flatMap(x3157 => join2.get({ val x3160 = x3157.o_orderkey 
x3160  }) match {
   case Some(a) => a.map(a1 => (a1, x3157))
   case _ => Nil
 })
} 
val x3163 = C__D._1 
val x3165 = x3163 
val x3174 = { val hm3212 = x3162.groupBy{case ((x3166, x3167), x3168) => { val x3170 = x3166.lbl 
val x3171 = x3170.C__F 
x3171   } }
 val join1 = x3165.flatMap{x3169 => hm3212.get(x3169._1) match {
 case Some(a) => x3169._2
 case _ => Nil
 }}
 val join2 = x3162.groupBy{case ((x3166, x3167), x3168) => { val x3173 = x3168.o_custkey 
x3173  } }
 join1.flatMap(x3169 => join2.get({ val x3172 = x3169.c_custkey 
x3172  }) match {
   case Some(a) => a.map(a1 => (a1, x3169))
   case _ => Nil
 })
} 
val x3185 = { val grps3213 = x3174.groupBy{ case (((x3175, x3176), x3177), x3178) => { val x3179 = (x3175) 
x3179  } }
 grps3213.toList.map(x3183 => (x3183._1, x3183._2.flatMap{ 
   case (((x3175, x3176), x3177), null) =>  Nil
   case (((x3175, x3176), x3177), x3178) => {val x3184 = (x3176,x3177,x3178) 
x3184 } match {
   case (null,null,_) => Nil
   case (x3176,x3177,x3178) => List({val x3180 = x3178.c_name 
val x3181 = x3178.c_nationkey 
val x3182 = Record3215(x3180, x3181, newId) 
x3182   })
 }
} ) ) } 
val x3190 = x3185.map{ case (x3186, x3187) => { 
  val x3188 = x3186.lbl 
  val x3189 = RecM_flat3(x3188, x3187, newId) 
  x3189 }} 
val M_flat3 = x3190
val x3191 = M_flat3
val x3192 = (x3038,x3070,x3083,x3124,x3137,x3191) 
x3192                                        }
    var end0 = System.currentTimeMillis() - start0
    
case class Input_Q3_Dict1(suppliers: (List[RecM_flat2], Unit), customers: (List[RecM_flat3], Unit))
val N__F = 7
val N__D = (List((N__F, TPCHLoader.loadNation[Nation].toList)), ())
val Query7__F = ShredQuery7._1.head.lbl
val Query7__D = (ShredQuery7._2, Input_Q3_Dict1((ShredQuery7._4, Unit), (ShredQuery7._6, Unit)))
    def f(){
      val x3231 = Q3Flat3(N__F, Query7__F, newId) 
val x3232 = Record3341(x3231, newId) 
val x3233 = List(x3232) 
val M_ctx1 = x3233
val x3234 = M_ctx1
val x3236 = M_ctx1 
val x3237 = N__D._1 
val x3239 = x3237 
val x3244 = { val hm3343 = x3236.groupBy{case x3240 => { val x3242 = x3240.lbl 
val x3243 = x3242.N__F 
x3243   } }
 x3239.flatMap{x3241 => hm3343.get(x3241._1) match {
 case Some(a) => a.map(a1 => (a1, x3241._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x3255 = { val grps3344 = x3244.groupBy{ case (x3245, x3246) => { val x3247 = (x3245) 
x3247  } }
 grps3344.toList.map(x3253 => (x3253._1, x3253._2.flatMap{ 
   case (x3245, null) =>  Nil
   case (x3245, x3246) => {val x3254 = (x3246) 
x3254 } match {
   case (null) => Nil
   case (x3246) => List({val x3248 = x3246.n_name 
val x3249 = x3245.lbl 
val x3250 = x3249.Query7__F 
val x3251 = Record3346(x3250, x3246, newId) 
val x3252 = Record3347(x3248, x3251, newId) 
x3252     })
 }
} ) ) } 
val x3260 = x3255.map{ case (x3256, x3257) => { 
  val x3258 = x3256.lbl 
  val x3259 = Record3348(x3258, x3257, newId) 
  x3259 }} 
val M_flat1 = x3260
val x3261 = M_flat1
val x3263 = M_flat1 
val x3267 = x3263.flatMap{ case x3264 => 
  val x3265 = x3264._2 
  x3265.flatMap(x3266 => {
    List((x3264, x3266))
})} 
val x3272 = x3267.map{ case (x3268, x3269) => { 
  val x3270 = x3269.part_names 
  val x3271 = Record3349(x3270, newId) 
  x3271 }} 
val x3273 = x3272.distinct 
val M_ctx2 = x3273
val x3274 = M_ctx2
val x3276 = M_ctx2 
val x3277 = Query7__D._1 
val x3279 = x3277 
val x3284 = { val hm3353 = x3276.groupBy{case x3280 => { val x3282 = x3280.lbl 
val x3283 = x3282.Query7__F 
x3283   } }
 x3279.flatMap{x3281 => hm3353.get(x3281._1) match {
 case Some(a) => a.map(a1 => (a1, x3281._2))
 case _ => Nil
}}.flatMap(v => v._2.map(v2 => (v._1, v2)))
} 
val x3285 = Query7__D._2 
val x3286 = x3285.suppliers 
val x3287 = x3286._1 
val x3289 = x3287 
val x3294 = { val hm3355 = x3289.map{ case x3292 => (x3292._1, x3292._2)}.toMap
x3284.flatMap{ 
  case (x3290, null) =>  List(((x3290, null), null))
  case (x3290, x3291) => hm3355.get({val x3293 = x3291.suppliers 
x3293 }) match {
   case Some(a) => a.flatMap{ case x3292 => 
     if ({true} == {true }) { List(((x3290, x3291), x3292)) } else { List(((x3290, x3291), null)) }
   }
   case _ => List(((x3290, x3291), null))
 }
}} 
val x3295 = x3285.customers 
val x3296 = x3295._1 
val x3298 = x3296 
val x3308 = { val hm3356 = x3294.groupBy{case ((x3299, x3300), x3301) => { val x3303 = x3300.customers 
x3303  } }
 val join1 = x3298.flatMap{x3302 => hm3356.get(x3302._1) match {
 case Some(a) => x3302._2
 case _ => Nil
 }}
 val join2 = x3294.groupBy{case ((x3299, x3300), x3301) => { val x3305 = x3299.lbl 
val x3306 = x3305.n__F 
val x3307 = x3306.n_nationkey 
x3307    } }
 join1.flatMap(x3302 => join2.get({ val x3304 = x3302.c_nationkey 
x3304  }) match {
   case Some(a) => a.map(a1 => (a1, x3302))
   case _ => Nil
 })
} 
val x3316 = { val grps3357 = x3308.groupBy{ case (((x3309, x3310), x3311), x3312) => { val x3313 = (x3309,x3310,x3311) 
x3313  } }
 grps3357.toList.map(x3314 => (x3314._1, x3314._2.foldLeft(0){ 
 case (acc3358, (((x3309, x3310), x3311), x3312)) => {val x3315 = (x3312) 
x3315 } match {
   case (null) => acc3358
   case _ => acc3358 + {1}
 }
} ) ) } 
val x3333 = { val grps3359 = x3316.groupBy{ case ((x3317, x3318, x3319), x3320) => { val x3321 = (x3317) 
x3321  } }
 grps3359.toList.map(x3324 => (x3324._1, x3324._2.flatMap{ 
   case ((x3317, x3318, x3319), 0) =>  Nil
   case ((x3317, x3318, x3319), x3320) => {val x3332 = (x3318,x3319) 
x3332 } match {
   case (null,_) => Nil
   case (x3318,x3319) => List({val x3322 = x3318.p_name 
val x3323 = Record3361(x3322, newId) 
x3323  })
 }
} ) ) } 
val x3338 = x3333.map{ case (x3334, x3335) => { 
  val x3336 = x3334.lbl 
  val x3337 = Record3362(x3336, x3335, newId) 
  x3337 }} 
val M_flat2 = x3338
val x3339 = M_flat2
val x3340 = (x3234,x3261,x3274,x3339) 
x3340                              
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
