package shredding.queries.tpch

import shredding.nrc.Printer

object App {

  def main (args: Array[String]){
    val printer = new Printer{}
    println(printer.quote(Queries.query1.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(Queries.query2.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(Queries.query3.asInstanceOf[printer.Expr]))
    println("")
    
  }

  


}
