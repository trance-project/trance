package shredding.examples.tpch

import shredding.nrc.Printer

object App {

  def main (args: Array[String]){
    val printer = new Printer{}
    println(printer.quote(TPCHQueries.query1.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TPCHQueries.query2.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TPCHQueries.query3.asInstanceOf[printer.Expr]))
    println("")
    
  }

  


}
