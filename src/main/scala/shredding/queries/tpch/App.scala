package shredding.queries.tpch

import shredding.nrc.Printer

object App {

  def main (args: Array[String]){
    val printer = new Printer{}
    println(printer.quote(TpchQueries.query1.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TpchQueries.query2.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TpchQueries.query3.asInstanceOf[printer.Expr]))
    println("")
    
  }

  


}
