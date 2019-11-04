
package experiments
/** Generated **/
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sprkloader._
import sprkloader.SkewPairRDD._
case class Record667(s__Fs_suppkey: Int)
case class Record668(s_name: String, customers2: Record667)
case class Record671(c_name2: String)

case class Record971(lbl: Unit)
case class Record972(o_orderkey: Int, o_custkey: Int)
case class Record973(c_name: String, c_custkey: Int)
case class Record975(o_orderkey: Int, c_name: String)
case class Record976(_1: Record971, _2: (Iterable[Record975]))
case class Record977(s_name: String, s_suppkey: Int)
case class Record979(s__Fs_suppkey: Int)
case class Record980(s_name: String, customers2: Record979)
case class Record981(_1: Record971, _2: (Iterable[Record980]))
case class Record982(lbl: Record979)
case class Record983(l_orderkey: Int, l_suppkey: Int)
case class Record985(c_name2: String)
case class Record986(_1: Record982, _2: (Iterable[Record985]))
case class Record1079(c_name: String)
case class Record1081(c__Fc_name: String)
case class Record1082(c_name: String, suppliers: Record1081)
case class Record1083(_1: Record971, _2: (Iterable[Record1082]))
case class Record1084(lbl: Record1081)
case class Record1086(s_name: String)
case class Record1087(_1: Record1084, _2: (Iterable[Record1086]))
object ShredQuery6FullDomainsSpark {
 def main(args: Array[String]){
   val sf = Config.datapath.split("/").last
   val conf = new SparkConf().setMaster(Config.master).setAppName("ShredQuery6FullDomainsSpark"+sf)
   val spark = SparkSession.builder().config(conf).getOrCreate()
   val tpch = TPCHLoader(spark)
/**val L__F = 3
val L__D_1 = tpch.loadLineitem
L__D_1.cache
L__D_1.count**/
val C__F = 1
val C__D_1 = tpch.loadCustomers
C__D_1.cache
C__D_1.count
/**val O__F = 2
val O__D_1 = tpch.loadOrders
O__D_1.cache
O__D_1.count
val S__F = 6
val S__D_1 = tpch.loadSupplier
S__D_1.cache
S__D_1.count

   val x847 = () 
val x848 = Record971(x847) 
val resultInner__F = x848
val x849 = resultInner__F
//resultInner__F.collect.foreach(println(_))
val x850 = List(resultInner__F) 
val x852 = x850 
val x853 = O__D_1 
val x858 = x853.map(x854 => { val x855 = x854.o_orderkey 
val x856 = x854.o_custkey 
val x857 = Record972(x855, x856) 
x857 }) 
val x861 = x858.map{ case c => (x852.head, c) } 
val x862 = C__D_1 
val x867 = x862.map(x863 => { val x864 = x863.c_name 
val x865 = x863.c_custkey 
val x866 = Record973(x864, x865) 
x866 }) 
val x873 = { val out1 = x861.map{ case (x868, x869) => ({val x871 = x869.o_custkey 
x871}, (x868, x869)) }
  val out2 = x867.map{ case x870 => ({val x872 = x870.c_custkey 
x872}, x870) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x883 = x873.flatMap{ case ((x874, x875), x876) => val x882 = (x875,x876) 
x882 match {
   case (_,null) => Nil 
   case x881 => List(({val x877 = (x874) 
x877}, {val x878 = x875.o_orderkey 
val x879 = x876.c_name 
val x880 = Record975(x878, x879) 
x880}))
 }
}.groupByLabel() 
val x888 = x883.map{ case (x884, x885) => 
   val x886 = (x885) 
val x887 = Record976(x884, x886) 
x887 
} 
val resultInner__D_1 = x888
val x889 = resultInner__D_1
//resultInner__D_1.collect.foreach(println(_))
val x890 = List(x848) 
val M_ctx1 = x890
val x891 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x893 = M_ctx1 
val x894 = S__D_1 
val x899 = x894.map(x895 => { val x896 = x895.s_name 
val x897 = x895.s_suppkey 
val x898 = Record977(x896, x897) 
x898 }) 
val x902 = x899.map{ case c => (x893.head, c) } 
val x912 = x902.flatMap{ case (x903, x904) => val x911 = (x904) 
x911 match {
   case (null) => Nil 
   case x910 => List(({val x905 = (x903) 
x905}, {val x906 = x904.s_name 
val x907 = x904.s_suppkey 
val x908 = Record979(x907) 
val x909 = Record980(x906, x908) 
x909}))
 }
}.groupByLabel() 
val x917 = x912.map{ case (x913, x914) => 
   val x915 = (x914) 
val x916 = Record981(x913, x915) 
x916 
} 
val M_flat1 = x917
val x918 = M_flat1
//M_flat1.collect.foreach(println(_))
val x920 = M_flat1 
val x924 = x920.flatMap{ case x921 => x921 match {
   case null => List((x921, null))
   case _ =>
   val x922 = x921._2 
x922 match {
     case x923 => x923.map{ case v2 => (x921, v2) }
  }
 }} 
val x929 = x924.map{ case (x925, x926) => 
   val x927 = x926.customers2 
val x928 = Record982(x927) 
x928 
} 
val x930 = x929.distinct 
val M_ctx2 = x930
val x931 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x933 = M_ctx2 
val x934 = L__D_1 
val x939 = x934.map(x935 => { val x936 = x935.l_orderkey 
val x937 = x935.l_suppkey 
val x938 = Record983(x936, x937) 
x938 }) 
val x945 = { val out1 = x933.map{ case x940 => ({val x942 = x940.lbl 
val x943 = x942.s__Fs_suppkey 
x943}, x940) }
  val out2 = x939.map{ case x941 => ({val x944 = x941.l_suppkey 
x944}, x941) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x946 = resultInner__D_1.flatMap{ r => r._2 }
val x948 = x946 
val x954 = { val out1 = x945.map{ case (x949, x950) => ({val x952 = x950.l_orderkey 
x952}, (x949, x950)) }
  //case class Record976(_1: Record971, _2: (Iterable[Record975]))
  val out2 = x948.map{ case x951 => ({val x953 = x951.o_orderkey 
x953}, x951) }
  out1.join(out2).map{ case (k,v) => v }
} 
val x963 = x954.flatMap{ case ((x955, x956), x957) => val x962 = (x956,x957) 
x962 match {
   case (_,null) => Nil 
   case x961 => List(({val x958 = (x955) 
x958}, {val x959 = x957.c_name 
val x960 = Record985(x959) 
x960}))
 }
}.groupByLabel() 
val x968 = x963.map{ case (x964, x965) => 
   val x966 = (x965) 
val x967 = Record986(x964, x966) 
x967 
} 
val M_flat2 = x968
val x969 = M_flat2
//M_flat2.collect.foreach(println(_))**/
val Query2Full__D_1 = spark.sparkContext.objectFile[Record668]("/nfs_qc4/query3/Query2Full__D_1Skew")//M_flat1
Query2Full__D_1.cache
Query2Full__D_1.count
val Query2Full__D_2customers2_1 = spark.sparkContext.objectFile[(Record667, Iterable[Record671])]("/nfs_qc4/query3/Query2Full__D_2customers2_1Skew")//M_flat2
Query2Full__D_2customers2_1.cache
Query2Full__D_2customers2_1.count
def f = { 
 val x999 = () 
val x1000 = Record971(x999) 
val x1001 = List(x1000) 
val M_ctx1 = x1001
val x1002 = M_ctx1
//M_ctx1.collect.foreach(println(_))
val x1004 = M_ctx1 
val x1005 = C__D_1 
val x1009 = x1005.map(x1006 => { val x1007 = x1006.c_name 
val x1008 = Record1079(x1007) 
x1008 }) 
val x1012 = x1009.map{ case c => (x1004.head, c) } 
val x1021 = x1012.flatMap{ case (x1013, x1014) => val x1020 = (x1014) 
x1020 match {
   case (null) => Nil 
   case x1019 => List(({val x1015 = (x1013) 
x1015}, {val x1016 = x1014.c_name 
val x1017 = Record1081(x1016) 
val x1018 = Record1082(x1016, x1017) 
x1018}))
 }
}.groupByLabel() 
val x1026 = x1021.map{ case (x1022, x1023) => 
   val x1024 = (x1023) 
val x1025 = Record1083(x1022, x1024) 
x1025 
} 
val M_flat1 = x1026
val x1027 = M_flat1
//M_flat1.collect.foreach(println(_))
val x1029 = M_flat1 
val x1033 = x1029.flatMap{ case x1030 => x1030 match {
   case null => List((x1030, null))
   case _ =>
   val x1031 = x1030._2 
x1031 match {
     case x1032 => x1032.map{ case v2 => (x1030, v2) }
  }
 }} 
val x1038 = x1033.map{ case (x1034, x1035) => 
   val x1036 = x1035.suppliers 
val x1037 = Record1084(x1036) 
x1037 
} 
val x1039 = x1038.distinct 
val M_ctx2 = x1039
val x1040 = M_ctx2
//M_ctx2.collect.foreach(println(_))
val x1042 = M_ctx2 
val x1043 = Query2Full__D_1//.flatMap{ r => r._2 }
val x1045 = x1043 
val x1048 = M_ctx2.cartesian(x1045)
val x1050 = Query2Full__D_2customers2_1 
val x1053 = x1050 
val x1058 = { val out1 = x1048.map{ case (a, null) => (null, (a, null)); case (x1054, x1055) => ({val x1057 = x1055.customers2 
x1057}, (x1054, x1055)) }
<<<<<<< HEAD
  val out2 = x1053.flatMap{ v2 => v2._2.map{ case x => (v2._1.lbl, x) } }//.flatMapValues(identity)
  out2.lookupSkewLeft(out1)
=======
  val out2 = x1053.flatMap{ v2 => v2._2.map{ case x => (v2._1, x) } }//.flatMapValues(identity)
  out1.lookup(out2)
>>>>>>> 04e29ed2c68d15cdb3211cd4adecc6e321b323ae
} 
val x1071 = x1058.flatMap{ case (x1061, (x1059, x1060)) => val x1070 = (x1059,x1060) 
x1070 match {
   case x1065 if {val x1066 = x1061.c_name2 
val x1067 = x1059.lbl 
val x1068 = x1067.c__Fc_name 
val x1069 = x1066 == x1068 
x1069} => List(({val x1062 = (x1059) 
x1062}, {val x1063 = x1060.s_name 
val x1064 = Record1086(x1063) 
x1064}))
   case x1065 => Nil //List(({val x1062 = (x1059) 
//x1062}, null))
 }    
}.groupByLabel() 
val x1076 = x1071.map{ case (x1072, x1073) => 
   val x1074 = (x1073) 
val x1075 = Record1087(x1072, x1074) 
x1075 
} 
val M_flat2 = x1076
val x1077 = M_flat2
//M_flat2.collect.foreach(println(_))
x1077.count
}
var start0 = System.currentTimeMillis()
f
var end0 = System.currentTimeMillis() - start0 
   println("ShredQuery6FullDomainsSpark"+sf+","+Config.datapath+","+end0+","+spark.sparkContext.applicationId)
 }
}
