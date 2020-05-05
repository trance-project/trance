package framework.examples.tpch

import framework.nrc.{Printer, MaterializeNRC}

object App {

  def main (args: Array[String]){
    val printer = new Printer with MaterializeNRC{}
    println(printer.quote(TPCHQueries.query1.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TPCHQueries.query2.asInstanceOf[printer.Expr]))
    println("")
    println(printer.quote(TPCHQueries.query3.asInstanceOf[printer.Expr]))
    println("")
    
  }

}
