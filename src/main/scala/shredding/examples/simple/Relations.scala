package shredding.examples.simple

import shredding.core._
import shredding.wmcc._

case class InputR(a: Int, b: String)
case class InputR3(n: Int)
case class InputR2(m: String, n: Int, k: List[InputR3])
case class InputR1(h: Int, j: List[InputR2])
case class InputRB2(c: Int)
case class InputRB1(a: Int, b: List[InputRB2])
case class InputRB1F(a: Int, b: Int)
case class InputRB1D(b: (List[(Int, List[InputRB2])], Unit))

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
    |import shredding.examples.simple._
    |val R = List(InputR(42, "Milos"), InputR(49, "Michael"), 
                  InputR(34, "Jaclyn"), InputR(42, "Thomas"))""".stripMargin
  
  val format1Fd = 1
  val format1Dd = (List((format1Fd, List(InputR(42, "Milos"), InputR(49, "Michael"),
                    InputR(34, "Jaclyn"), InputR(42, "Thomas")))), ())
  
  val format1e = s"""
    |import shredding.examples.simple._
    |val R__F = 1
    |val R__D = (List((R__F, List(InputR(42, "Milos"), InputR(49, "Michael"), 
    |             InputR(34, "Jaclyn"), InputR(42, "Thomas")))), ())""".stripMargin
}

object NestedRelations{

  // Nested Relation 1: Bag(h: Int, j: Bag(m: String, n: Int, k: Bag(n: Int)))
  val type3a = TupleType(Map("n" -> IntType))
  val type2a = TupleType(Map("m" -> StringType, "n" -> IntType, "k" -> BagType(type3a)))
  val type1a = TupleType(Map("h" -> IntType, "j" -> BagType(type2a)))

  // type map for scala code generation
  val nested1Inputs:Map[Type,String] = Map(type1a -> "InputR1", type2a -> "InputR2", type3a -> "InputR3")

  val format1a = List(Map(
          "h" -> 42,
          "j" -> List(
            Map(
              "m" -> "Milos",
              "n" -> 123,
              "k" -> List(
                Map("n" -> 123),
                Map("n" -> 456),
                Map("n" -> 789),
                Map("n" -> 123)
              )
            ),
            Map(
              "m" -> "Michael",
              "n" -> 7,
              "k" -> List(
                Map("n" -> 2),
                Map("n" -> 9),
               Map("n" -> 1)
              )
            ),
            Map(
              "m" -> "Jaclyn",
              "n" -> 12,
              "k" -> List(
                Map("n" -> 14),
                Map("n" -> 12)
              )
            )
          )
        ),
        Map(
          "h" -> 69,
          "j" -> List(
            Map(
              "m" -> "Thomas",
              "n" -> 987,
              "k" -> List(
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 654),
                Map("n" -> 987),
                Map("n" -> 987)
              )
            )
          )
        )
      )

    val format1b = List(InputR1(42, List(InputR2("Milos", 123, List(InputR3(123), InputR3(456), InputR3(789), InputR3(123))),
                                 InputR2("Michael", 7, List(InputR3(2), InputR3(9), InputR3(1))),
                                 InputR2("Jaclyn", 12, List(InputR3(14), InputR3(12))))),
                InputR1(69, List(InputR2("Thomas", 987, List(InputR3(987), InputR3(654), InputR3(987), InputR3(987))))))
    /**val R__F = 1
    case class InputR1Flat(a: Int, b: Int)
    case class InputR2Flat(m: String, n: Int, k: Int)
    case class InputR3Flat(n: Int)
    case class InputR2Dict(k: (List[(Int, List[InputR3Flat])], Unit))
    case class InputR1Dict(j: (List[(Int, List[InputR2Flat])], InputR2Dict))
    val R__D = (List((R__F, List(InputR1Flat(42, 2), InputR1Flat(69, 3)))),
      InputR1Dict(j: (List((2, List(InputR2Flat("Milos", 123, 4), InputR2Flat("Michael", 7, 5),
                                    InputR2Flat("Jaclyn", 12, 6))),
                            (3, List(InputR2Flat("Thomas", 987, 7)))),
      InputR2Dict(k: (List((4, List(InputR3Flat(123), InputR3Flat(456), InputR3Flat(789), InputR3Flat(123))),
                            (5, List(InputR3Flat(2), InputR3Flat(9), InputR3Flat(1))),
                            (6, List(InputR3Flat(14), InputR3Flat(12))),
                            (7, List(InputR3Flat(987), InputR3Flat(654), InputR3Flat(987), InputR3Flat(987)))), ())))))
    **/
    
    // Bag(a: Int, b: Bag(c: Int))
    val type22a = TupleType("c" -> IntType) 
    val type21a = TupleType("a" -> IntType, "b" -> BagType(type22a))
    
    val nested2Inputs:Map[Type, String] = Map(type22a -> "InputRB1", type21a -> "InputRB2")
    val nested2SInputs:Map[Type, String] = Map(type22a -> "InputRB1", TupleType("a" -> IntType, "b" -> IntType) -> "IntputRB1F", BagDictType(BagType(TupleType("c" -> IntType)), TupleDictType(Map[String, TupleDictAttributeType]())) -> "InputRB1D")
   
   val format2a = List(RecordValue("a" -> 42, "b" -> List(RecordValue("c" -> 1), RecordValue("c" -> 2), RecordValue("c" -> 4))),
                              RecordValue("a" -> 49, "b" -> List(RecordValue("c" -> 3), RecordValue("c" -> 2))),
                              RecordValue("a" -> 34, "b" -> List(RecordValue("c" ->5))))
    
    val format2b = List(InputRB1(42,  List(InputRB2(1), InputRB2(2), InputRB2(4))),
                        InputRB1(49,  List(InputRB2(3), InputRB2(2))),
                        InputRB1(34,  List(InputRB2(5))))
    val format2c = s"""
      |import shredding.examples.simple._
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
      |import shredding.examples.simple._
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
    
    
    val type4b = TupleType("c" -> IntType, "d" -> IntType)
    val type4e = TupleType("f" -> IntType, "g" -> IntType)
    val type4a = TupleType("a" -> StringType, "b" -> BagType(type4b), "e" -> BagType(type4e))
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
