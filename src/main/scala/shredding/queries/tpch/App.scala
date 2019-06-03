package shredding.queries.tpch

import shredding.nrc.Printer

object App {

  def main (args: Array[String]){
    val printer = new Printer{}
    val queries = new Queries{}
    println(printer.quote(queries.query1.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(queries.query2.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(queries.query3.asInstanceOf[printer.Expr]))
    println("")
    
  }

  


}
