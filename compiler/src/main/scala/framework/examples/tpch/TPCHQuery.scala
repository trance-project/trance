package framework.examples.tpch

import framework.common._
import framework.examples.Query
import framework.utils.Utils.Symbol


/** Base trait for a TPCH query. 
  * Provides support functions for projection tuples, and referencing base tables.
  */
trait TPCHBase extends Query {

  // schema information
  val tbls: Set[String]

  def loaders: Map[String, String] = Map(
    "Customer" -> "loadCustomerDF()",
    "Order" -> "loadOrderDF()",
    "Lineitem" -> "loadLineitemDF()",
    "Part" -> "loadPartDF()",
    "PartSupp" -> "loadPartSuppDF()",
    "Supplier" -> "loadSupplierDF()",
    "Nation" -> "loadNationDF()",
    "Region" -> "loadRegionDF()")

  val loaderName = "tpch"

  def loadTable(tbl: String, shred: Boolean = false, skew: Boolean = false): String = {
    val tblName = if (shred) s"IBag_${tbl}__D" else tbl
    val tblCall = if (skew)  
      s"""|val ${tblName}_L = $loaderName.${loaders(tbl)}
          |val $tblName = (${tblName}_L, ${tblName}_L.empty)"""
      else s"""|val $tblName = $loaderName.${loaders(tbl)}"""
    s"""$tblCall
        |$tblName.cache
        |$tblName.count
        |""".stripMargin
  }

  def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
      s"""|val $loaderName = TPCHLoader(spark)
          |${loaders.filter(f => tbls(f._1)).map(f => loadTable(f._1, shred, skew)).mkString("\n")}
          |""".stripMargin
  }

  /** Input references **/

  val relC = BagVarRef("Customer", TPCHSchema.customertype)
  val cr = TupleVarRef("c", TPCHSchema.customertype.tp)

  val relO = BagVarRef("Order", TPCHSchema.orderstype)
  val or = TupleVarRef("o", TPCHSchema.orderstype.tp)

  val relL = BagVarRef("Lineitem", TPCHSchema.lineittype)
  val lr = TupleVarRef("l", TPCHSchema.lineittype.tp)

  val relP = BagVarRef("Part", TPCHSchema.parttype)
  val pr = TupleVarRef("p", TPCHSchema.parttype.tp)

  val relS = BagVarRef("Supplier", TPCHSchema.suppliertype)
  val sr = TupleVarRef("s", TPCHSchema.suppliertype.tp)
  
  val relPS = BagVarRef("PartSupp", TPCHSchema.partsupptype)
  val psr = TupleVarRef("ps", TPCHSchema.partsupptype.tp)

  val relN = BagVarRef("Nation", TPCHSchema.nationtype)
  val nr = TupleVarRef("n", TPCHSchema.nationtype.tp)

  val relR = BagVarRef("Region", TPCHSchema.regiontype)
  val rr = TupleVarRef("r", TPCHSchema.regiontype.tp)

  /** Helper functions for writing queries with and without projections **/
  
  def projectBaseTuple(tr: TupleVarRef, omit: List[String] = Nil): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1))))

  def projectTuples(tr1: TupleVarRef, tr2: TupleVarRef, omit: List[String] = Nil): BagExpr = {
    val m1 = tr1.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr1(f._1))
    val m2 = tr2.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr2(f._1))
    Singleton(Tuple(m1 ++ m2))
  }

  def projectTuple(tr: TupleVarRef, nbag:Map[String, TupleAttributeExpr], omit: List[String]): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) ++ nbag))

  def projectTuple(tr: TupleVarRef, nbag:(String, TupleAttributeExpr), omit: List[String] = Nil): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) + nbag))



}
