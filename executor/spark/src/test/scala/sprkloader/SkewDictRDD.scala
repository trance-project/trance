package sprkloader

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing._

case class Label1(id: Int)
case class Label2(id: Int)
case class COP1(cname: String, corder: Label1)
case class COP2(date: String, opart: Label2)
case class COP3(part: String, qty: Double)


class TestSkewDictRDD extends FunSuite with SharedSparkContext {
  
  // test("sanity check") {
  //   val list = List(1, 2, 3, 4)
  //   val rdd = sc.parallelize(list)
  //   assert(rdd.count === list.length)
  // }

  // val l1 = List(
  //     COP1("cname1", Label1(1)), 
  //     COP1("cname1", Label1(1)), 
  //     COP1("cname1", Label1(2)),
  //     COP1("cname2", Label1(2)),
  //     COP1("cname3", Label1(3)))

  // val l2 = List(
  //     (Label1(1), Iterable(COP2("date1", Label2(1)), COP2("date2", Label2(2)))),
  //     (Label1(2), Iterable(COP2("date3", Label2(1)), COP2("date4", Label2(2)))), 
  //     (Label1(4), Iterable(COP2("date5", Label2(3)))))

  // val l3 = List(
  //     (Label2(1), Iterable(COP3("part1", 1.0), COP3("part2", 2.0), COP3("part3", 3.0))),
  //     (Label2(2), Iterable(COP3("part1", 2.0), COP3("part2", 4.0))),
  //     (Label2(4), Iterable(COP3("part3", 4.0))))
  
  
  // test("unshredding"){
  //   val dict1 = sc.parallelize(l1)
  //   val dict2 = sc.parallelize(l2)
  //   val dict3 = sc.parallelize(l3)
    
  //   val tmp = dict2.flatMap{ case (lbl, bag) => 
  //     bag.map(b => b.opart -> (lbl, b.date))
  //   }.cogroup(dict3).flatMap{
  //     case (_, (d2s, d3s)) => d2s.map(d => (d._1, (d._2, d3s.flatten)))
  //   }

  //   val result = dict1.map(c => c.corder -> c.cname).cogroup(tmp).flatMap{
  //     case (_, (names, dates)) => names.map(c => (c, dates))
  //   }
    
  //   val flatten = result.zipWithIndex.flatMap{ case (c, index) =>  
  //     if (c._2.isEmpty) List((index, c._1, null, null, null))
  //     else c._2.zipWithIndex.flatMap{ case (o, oindex) => 
  //       if (o._2.isEmpty) List((index, c._1, oindex, o._1, null, null))
  //       else o._2.map(p => (index, c._1, oindex, o._1, p.part, p.qty))}}
    
  //   result.collect.foreach(println(_))
  //   flatten.collect.foreach(println(_)) 
  //   assert(result.count == 5)

  // }


  // test("dict lookup domain"){
  //   import DomainRDD._
  //   import SkewDictRDD._

  //   val dict1 = sc.parallelize(l1)
  //   val dict2 = sc.parallelize(l2)
  //   //val dict3 = sc.parallelize(l3)

  //   val fdict1 = dict1.map(c => (c.corder, c))
  //   val domain = dict1.createDomain(l => l.corder)
  //   assert(domain.count == 5)
  //   assert(domain.collect.toSet == Set(Label1(1), Label1(2), Label1(3)))
  //   val result = dict2.lookupIterator(domain, (l: Label1) => l)
  //   assert(result.count == 4)
  //   assert(result.filter(_._1 == Label1(4)).count == 0) 
  // }


  // test("dict lookup dict"){
  //   import DomainRDD._
  //   import SkewDictRDD._

  //   val dict1 = sc.parallelize(l1)
  //   val dict2 = sc.parallelize(l2)
  //   val dict3 = sc.parallelize(l3)

  //   val fdict1 = dict1.map(c => (c.corder, c))
  //   val domain = dict1.createDomain(l => l.corder)
  //   assert(domain.count == 5)
  //   assert(domain.collect.toSet == Set(Label1(1), Label1(2), Label1(3)))
  //   val result = dict2.lookupIterator(domain, (l: Label1) => l)
  //   assert(result.count == 4)
  //   assert(result.filter(_._1 == Label1(4)).count == 0) 
  // }

}
