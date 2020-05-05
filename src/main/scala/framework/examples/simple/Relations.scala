package framework.examples.simple

import framework.core._
import framework.plans._

case class InputR(a: Int, b: String)
case class InputR3(n: Int)
case class InputR2(m: String, n: Int, k: List[InputR3])
case class InputR1(h: Int, j: List[InputR2])
case class InputRB2(c: Int)
case class InputRB1(a: Int, b: List[InputRB2])
case class InputRB1F(a: Int, b: Int)
case class InputRB1D(b: (List[(Int, List[InputRB2])], Unit))

case class RecordValue3(f: Int, g: Int)
case class RecordValue2(c: Int, d: Int)
case class RecordValue1(a: Int, b: List[RecordValue2], e:List[RecordValue3])

case class InputS2(b: Int, c: Int)
case class InputS1(b: Int, s1: List[InputS2], s2: List[InputS2])

case class P(p_partkey: Int, p_name: String)
case class PS(ps_partkey: Int, ps_suppkey: Int)
case class S(s_suppkey: Int, s_name: String, s_nationkey: Int)
case class L(l_partkey: Int, l_orderkey: Int)
case class O(o_orderkey: Int, o_custkey: Int)
case class C(c_custkey: Int, c_name: String, c_nationkey: Int)

/**
  * Flat and nested relations used for simple queries
  * each relation starts with type information, then has the 
  * following data formats:
  *   format#a: source NRC input format
  *   format#b: wmcc format used for generated scala code
  *   format#c: format#b instring format used for generated scala code
  *   format#d: shred wmcc format used for generated scala code
  *   format#e: format#e in string format used for generated scala code
  */
object FlatRelations {

  // Relation 1: Bag(a: Int, b: String)  
  val type1a = TupleType("a" -> IntType, "b" -> StringType)  
  val format1a = List(RecordValue("a" -> 42, "b" -> "Milos"), RecordValue("a" -> 49, "b" -> "Michael"),
                           RecordValue("a" -> 34, "b" -> "Jaclyn"), RecordValue("a" -> 42, "b" -> "Thomas"))

  val type1b = RecordCType("a" -> IntType, "b" -> StringType)
  val format1b = List(InputR(42, "Milos"), InputR(49, "Michael"), InputR(34, "Jaclyn"), InputR(42, "Thomas")) 

  val format1c = s"""
    |import framework.examples.simple._
    |val R = List(InputR(42, "Milos"), InputR(49, "Michael"), 
                  InputR(34, "Jaclyn"), InputR(42, "Thomas"))""".stripMargin
  
  val format1Fd = 1
  val format1Dd = (List((format1Fd, List(InputR(42, "Milos"), InputR(49, "Michael"),
                    InputR(34, "Jaclyn"), InputR(42, "Thomas")))), ())
  
  val format1e = s"""
    |import framework.examples.simple._
    |val R__F = 1
    |val R__D = (List((R__F, List(InputR(42, "Milos"), InputR(49, "Michael"), 
    |             InputR(34, "Jaclyn"), InputR(42, "Thomas")))), ())""".stripMargin
  
  val format1Spark = s"""
    |val R = spark.sparkContext.parallelize(List(Input_R(42, "Milos"), Input_R(49, "Michael"),
    |              Input_R(34, "Jaclyn"), Input_R(42, "Thomas")))""".stripMargin

  val format2Spark = s"""
    |val R__F = 1
    |val R__D_1 = spark.sparkContext.parallelize(List(Input_R__D(42, "Milos"), Input_R__D(49, "Michael"),
    |              Input_R__D(34, "Jaclyn"), Input_R__D(42, "Thomas"))).map(m => (R__F, m))""".stripMargin

}

object NestedRelations{

  // Nested Relation 1: Bag(h: Int, j: Bag(m: String, n: Int, k: Bag(n: Int)))
  val type3a = TupleType(Map("n" -> IntType))
  val type2a = TupleType(Map("m" -> StringType, "n" -> IntType, "k" -> BagType(type3a)))
  val type1a = TupleType(Map("h" -> IntType, "j" -> BagType(type2a)))

  // type map for scala code generation
  val nested1Inputs:Map[Type,String] = Map(type1a -> "InputR1", type2a -> "InputR2", type3a -> "InputR3")

  val format1a = List(RecordValue(
          "h" -> 42,
          "j" -> List(
            RecordValue(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                RecordValue("n" -> 123),
                RecordValue("n" -> 456),
                RecordValue("n" -> 789),
                RecordValue("n" -> 123)
              )
            ),
            RecordValue(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                RecordValue("n" -> 2),
                RecordValue("n" -> 9),
               RecordValue("n" -> 1)
              )
            ),
            RecordValue(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                RecordValue("n" -> 14),
                RecordValue("n" -> 12)
              )
            )
          )
        ),
        RecordValue(
          "h" -> 69,
          "j" -> List(
            RecordValue(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                RecordValue("n" -> 987),
                RecordValue("n" -> 654),
                RecordValue("n" -> 987),
                RecordValue("n" -> 654),
                RecordValue("n" -> 987),
                RecordValue("n" -> 987)
              )
            )
          )
        )
      )

    val format1b = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
                                 InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
                                 InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))),
                InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987), InputR3(987), InputR3(987))))))

    val format1Spark = s"""
      |val R = spark.sparkContext.parallelize(List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
      |                           InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
      |                          InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))),
      |          InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987)))))))""".stripMargin
    
    
    val format2Spark = s"""
      |val R = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
      |                           InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
      |                          InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))),
      |          InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987))))))
      |case class Flat2(m: String, n: Int, k: Int)
      |case class Flat3(n: Int)
      |val R__F = 1
      |val R__D_1 = spark.sparkContext.parallelize(List((R__F, R.map{ case t => Input_R__D(t.h, t.hashCode)})))
      |val R__D_2j_1 = spark.sparkContext.parallelize(R.map{ case t => (t.hashCode, t.j.map{ case t2 => Flat2(t2.m, t2.n, t2.hashCode) }) })
      |val R__D_2j_2k_1 = spark.sparkContext.parallelize(R.map{ case t => t.j.map{ case t3 => (t3.hashCode, t3.k.map{ case t4 => Flat3(t4.n) }) } })
    """
    
    // Bag(a: Int, b: Bag(c: Int))
    val type22a = TupleType("c" -> IntType) 
    val type21a = TupleType("a" -> IntType, "b" -> BagType(type22a))
    
    val nested2Inputs: Map[Type, String] =
      Map(type22a -> "InputRB1", type21a -> "InputRB2")

    // TODO: check LabelType()
    val nested2SInputs: Map[Type, String] =
      Map(
        type22a -> "InputRB1",
        TupleType("a" -> IntType, "b" -> IntType) -> "IntputRB1F",
        BagDictType(
          LabelType(),
          BagType(TupleType("c" -> IntType)),
          TupleDictType(Map[String, TupleDictAttributeType]())
        ) -> "InputRB1D"
      )
   
    val format2a = List(RecordValue("a" -> 42, "b" -> List(RecordValue("c" -> 1), RecordValue("c" -> 2), RecordValue("c" -> 4))),
                              RecordValue("a" -> 49, "b" -> List(RecordValue("c" -> 3), RecordValue("c" -> 2))),
                              RecordValue("a" -> 34, "b" -> List(RecordValue("c" ->5))))
    
    val format2aSpark = s"""
      |val R = spark.sparkContext.parallelize(List(InputRB1(42, List(InputRB2(1), InputRB2(2), InputRB2(4))),
                              InputRB1(49, List(InputRB2(3), InputRB2(2))),
                              InputRB1(34, List(InputRB2(5)))))""".stripMargin
    
    val format2bSpark = s"""
      |val R = List(InputRB1(42, List(InputRB2(1), InputRB2(2), InputRB2(4))),
      |                        InputRB1(49, List(InputRB2(3), InputRB2(2))),
      |                        InputRB1(34, List(InputRB2(5))))
      |val R__F = 1
      |val R__D_1 = spark.sparkContext.parallelize(List((R__F, R.map{ case t => Input_R__D(t.a, t.hashCode)})))
      |val R__D_2b_1 = spark.sparkContext.parallelize(R.map{ case t => (t.hashCode, t.b) })""".stripMargin

    val format2b = List(InputRB1(42,  List(InputRB2(1), InputRB2(2), InputRB2(4))),
                        InputRB1(49,  List(InputRB2(3), InputRB2(2))),
                        InputRB1(34,  List(InputRB2(5))))
    val format2c = s"""
      |import framework.examples.simple._
      |val R =  List(InputRB1(42,  List(InputRB2(1), InputRB2(2), InputRB2(4))),
      |              InputRB1(49,  List(InputRB2(3), InputRB2(2))),
      |              InputRB1(34,  List(InputRB2(5))))""".stripMargin

    val format2Fd = 1
    val tmp1 = (List((2, List(InputRB2(1), InputRB2(2), InputRB2(4))),
                    (3, List(InputRB2(3), InputRB2(2))),
                    (4,  List(InputRB2(5)))), ())
    val format2Dd = (List((format2Fd, List(InputRB1F(42,  2), InputRB1F(49, 3), InputRB1F(34, 4)))),
                      InputRB1D(tmp1))
    val format2De = s"""
      |import framework.examples.simple._
      |val R = (List((format2Fd, List(InputRB1F(42,  2), InputRB1F(49, 3), InputRB1F(34, 4)))),
      |                InputRB1D(b: (List((2, List(InputRB2(1), InputRB2(2), InputRB2(4))),
      |                                   (3, List(InputRB2(3), InputRB2(2))),
      |                                   (4,  List(InputRB2(5)))), ())))""".stripMargin

    // Bag(a: Int, s1: Bag(b: Int, c: Int), s2: Bag(b: Int, c: Int))
    val type32a = TupleType("b" -> IntType, "c" -> IntType)
    val type31a = TupleType("a" -> IntType, "s1" -> BagType(type32a), "s2" -> BagType(type32a))

    val format3a = List(Map("a" -> 1, "s1" -> List(Map("b" -> 12, "c" -> 3), Map("b" -> 4, "c" -> 2)),
                        "s2" -> List(Map("b" -> 16, "c" -> 11), Map("b" -> 6, "c" -> 6))), 
                        Map("a" -> 10, "s1" -> List(Map("b" -> 5, "c" -> 20)),
                        "s2" -> List(Map("b" -> 11, "c" -> 16), Map("b" -> 2, "c" -> 50))))
   val format3aSpark = s"""
    | val R = spark.sparkContext.parallelize(List(InputS1(1, List(InputS2(12, 3), InputS2(4, 2)),
    |                    List(InputS2(16, 11), InputS2(6, 6))), 
    |                    InputS1(10, List(InputS2(5, 20)),
    |                    List(InputS2(11, 16), InputS2(2, 50)))))""".stripMargin
   
    val format3Spark = s"""
      |val R = spark.sparkContext.parallelize(List(RecordValue1(\"part1\", List(RecordValue2(12, 3), RecordValue2(4, 2)),
      |                  List(RecordValue3(16, 11), RecordValue3(6, 6))), 
      |     RecordValue1(\"part10\", List(RecordValue2(5, 20)),
      |                  List(RecordValue3(11, 16), RecordValue3(2, 50)))))""".stripMargin
       
    val type4b = TupleType("c" -> IntType, "d" -> IntType)
    val type4e = TupleType("f" -> IntType, "g" -> IntType)
    val type4a = TupleType("a" -> StringType, "b" -> BagType(type4b), "e" -> BagType(type4e))
   
    var q10inputs: Map[Type, String] = Map(type4b -> "RecordValue2", 
                                           type4e -> "RecordValue3", 
                                           type4a -> "RecordValue1")
   
    val format4a = List(RecordValue("a" -> "part", 
                                    "b" -> List(RecordValue("c" -> 1,"d" -> 17), 
                                                RecordValue("c" -> 1,"d" -> 17),
                                                RecordValue("c" -> 1,"d" -> 17),
                                                RecordValue("c" -> 1,"d" -> 17)), 
                                    "e" -> List(RecordValue("f" -> 1, "g" -> 15), 
                                                RecordValue("f" -> 1, "g" -> 15),
                                                RecordValue("f" -> 1, "g" -> 15),
                                                RecordValue("f" -> 1, "g" -> 15),
                                                RecordValue("f" -> 1, "g" -> 15),
                                                RecordValue("f" -> 1, "g" -> 15)))) 
 
    val typeP = TupleType("p_partkey" -> IntType, "p_name" -> StringType)
    val typePS = TupleType("ps_partkey" -> IntType, "ps_suppkey" -> IntType)
    val typeS = TupleType("s_suppkey" -> IntType, "s_name" -> StringType, "s_nationkey" -> IntType)
    val typeL = TupleType("l_partkey" -> IntType, "l_orderkey" -> IntType)
    val typeO = TupleType("o_orderkey" -> IntType, "o_custkey" -> IntType)
    val typeC = TupleType("c_custkey" -> IntType, "c_name" -> StringType, "c_nationkey" -> IntType)

    val format4Spark = 
      s"""
        val P = spark.sparkContext.parallelize(List(Input_P(1, "part1", newId), Input_P(2, "part2", newId), 
                  Input_P(3, "part3", newId), Input_P(4, "part4", newId), Input_P(5, "part5", newId)))
        val PS = spark.sparkContext.parallelize(List(Input_PS(1, 1, newId), Input_PS(2, 1, newId), Input_PS(3, 2, newId), 
                  Input_PS(4, 3, newId), Input_PS(5, 3, newId), Input_PS(1, 4, newId)))
        val S = spark.sparkContext.parallelize(List(Input_S(1, "supplier A", 1, newId), Input_S(2, "supplier B", 1, newId), 
                  Input_S(3, "supplier C", 2, newId), Input_S(4, "supplier D", 3, newId)))
        val L = spark.sparkContext.parallelize(List(Input_L(1, 1, newId), Input_L(1, 2, newId), Input_L(2, 1, newId), Input_L(2, 3, newId), Input_L(3, 1, newId), Input_L(4, 5, newId), Input_L(5, 4, newId)))
        val O = spark.sparkContext.parallelize(List(Input_O(1, 1, newId), Input_O(2, 1, newId), Input_O(3, 2, newId), Input_O(4, 3, newId), Input_O(5, 2, newId)))
        val C = spark.sparkContext.parallelize(List(Input_C(1, "Test Customer 1", 1, newId), Input_C(2, "Test Customer 2", 1, newId), Input_C(3, "Test Customer 3", 1, newId), Input_C(4, "Test Customer 4", 2, newId), Input_C(5, "Test Customer 5", 3, newId), Input_C(6, "Test Customer 6", 3, newId)))
        val R = spark.sparkContext.parallelize(List(RecordValue1("part1", 
                            List(RecordValue2(1,17), 
                                 RecordValue2(1,17),
                                 RecordValue2(1,17),
                                 RecordValue2(1,17)),
                            List(RecordValue3(1, 15), 
                                 RecordValue3(1, 15),
                                 RecordValue3(1, 15),
                                 RecordValue3(1, 15),
                                 RecordValue3(1, 15),
                                 RecordValue3(1, 15)))))"""
                                       
    val sformat4a = (List((1, List(RecordValue("a" -> "part", "b" -> 2, "e" -> 3)))),
                      RecordValue("b" -> (List((2, List(RecordValue("c" -> 1,"d" -> 17),
                                                               RecordValue("c" -> 1,"d" -> 17),
                                                               RecordValue("c" -> 1,"d" -> 17),
                                                               RecordValue("c" -> 1,"d" -> 17)))), ()),
                                  "e" -> (List((3, List(RecordValue("f" -> 1, "g" -> 15),
                                                               RecordValue("f" -> 1, "g" -> 15),
                                                               RecordValue("f" -> 1, "g" -> 15),
                                                               RecordValue("f" -> 1, "g" -> 15),
                                                               RecordValue("f" -> 1, "g" -> 15),
                                                               RecordValue("f" -> 1, "g" -> 15)))), ())))
}
