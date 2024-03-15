package uk.ac.ox.cs.trance

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.TPCHDataframes.{Customer => CustomerDF, LineItem => LineItemDF, Nation => NationDF, Order => OrderDF, Part => PartDF, Region => RegionDF, Supplier => SupplierDF}
import uk.ac.ox.cs.trance.utilities._
import Wrapper.DataFrameImplicit

class LibraryCompilerComparisonTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {

  val customer = CustomerDF
  val wrappedCustomer = customer.wrap()

  val lineItem = LineItemDF
  val wrappedLineItem = lineItem.wrap()

  val order = OrderDF
  val wrappedOrder = order.wrap()

  val part = PartDF
  val wrappedPart = part.wrap()

  val nation = NationDF
  val wrappedNation = nation.wrap()

  val region = RegionDF
  val wrappedRegion = region.wrap()

  val supplier = SupplierDF
  val wrappedSupplier = supplier.wrap()

  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  def assertDataFrameSchemaEqual(df1: DataFrame, df2: DataFrame): Unit = {
    val schema1 = df1.schema.fields.map(f => f.copy(nullable = false))
    val schema2 = df2.schema.fields.map(f => f.copy(nullable = false))

    schema1 shouldEqual schema2
  }

  def assertDataFramesAreEquivalentOld(df1: DataFrame, df2: DataFrame): Unit = {
    implicit val anyOrdering: Ordering[Any] = Ordering.fromLessThan {
      case (a, b) => a.toString < b.toString
    }

    val count1 = df1.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)
    val count2 = df2.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)

    count1 shouldEqual count2
  }

  def assertDataFramesAreEquivalent(df1: DataFrame, df2: DataFrame): Unit = {
    // Normalize the column order by selecting columns from df1 in the order they appear in df2
    val orderedDf1 = df1.select(df2.columns.map(df1.col): _*)

    // Convert DataFrames to RDDs of Strings to normalize content representation,
    // ignoring the order of elements within arrays by converting them to sets
    def dfToStringRows(df: DataFrame): Array[String] = {
      df.rdd.map(row => {
        val normalizedRow = row.toSeq.map {
          // Convert arrays to sorted strings to make them order invariant
          case arr: Seq[_] => arr.map(_.toString).sorted.mkString("[", ",", "]")
          case value => value.toString
        }.mkString(",")
        normalizedRow
      }).collect().sorted
    }

    val stringRows1 = dfToStringRows(orderedDf1)
    val stringRows2 = dfToStringRows(df2)

    // Assert equivalence of string representations
    assert(stringRows1.sameElements(stringRows2), "DataFrames are not equivalent")
  }

  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatToNested") {
    it("Test0") {
      val gen = GeneratedCodeTests.Test0().toDF()
      val lib = LibraryFlatToNestedTests.Test0()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test0Full") {
      val gen = GeneratedCodeTests.Test0Full().toDF()
      val lib = LibraryFlatToNestedTests.Test0Full()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test1") {
      val gen = GeneratedCodeTests.Test1().drop("Order_index").toDF()
      val lib = LibraryFlatToNestedTests.Test1()

      assertDataFramesAreEquivalent(gen, lib)
    }

    // Projecting base tuple of LineItem contains extraenous uniqueId column, circumvented by using omit() on uniqueId in query
    it("Test1Full") {
      val gen = GeneratedCodeTests.Test1Full().drop("Order_index").toDF()
      val lib = LibraryFlatToNestedTests.Test1Full()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2") {
      val gen = GeneratedCodeTests.Test2().drop("Customer_index").toDF()
      val lib = LibraryFlatToNestedTests.Test2()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2Filter") {
      val gen = GeneratedCodeTests.Test2Filter().drop("Customer_index").toDF()
      val lib = LibraryFlatToNestedTests.Test2Filter()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2Full") {
      val gen = GeneratedCodeTests.Test2Full().drop("Customer_index")
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test2Full()
      gen.show(false)
      lib.show(false)
      gen.printSchema()
      lib.printSchema()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2Flat") {
      val gen = GeneratedCodeTests.Test2Flat().drop("Customer_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test2Flat()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test3") {
      val gen = GeneratedCodeTests.Test3().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test3Full") {
      val gen = GeneratedCodeTests.Test3Full().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3Full()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test3Flat") {
      val gen = GeneratedCodeTests.Test3Flat().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3Flat()

      assertDataFramesAreEquivalent(gen, lib)
    }


    it("Test3FullFlat") {
      val gen = GeneratedCodeTests.Test3FullFlat().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3FullFlat()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4") {
      val gen = GeneratedCodeTests.Test4().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4Full") {
      val gen = GeneratedCodeTests.Test4Full().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4Full()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4Flat") {
      val gen = GeneratedCodeTests.Test4Flat().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4Flat()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4FullFlat") {
      val gen = GeneratedCodeTests.Test4FullFlat().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4FullFlat()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test0Join") {
      val gen = GeneratedCodeTests.Test0Join().toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test0Join()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test1Join") {
      val gen = GeneratedCodeTests.Test1Join().toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test1Join()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test1JoinFlat") {
      val gen = GeneratedCodeTests.Test1JoinFlat().drop("Order_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test1JoinFlat()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2Join") {
      val gen = GeneratedCodeTests.Test2Join().drop("Customer_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test2Join()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test2JoinFlat") {
      val gen = GeneratedCodeTests.Test2JoinFlat().drop("Customer_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test2JoinFlat()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test3Join") {
      val gen = GeneratedCodeTests.Test3Join().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3Join()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test3JoinFlat") {
      val gen = GeneratedCodeTests.Test3JoinFlat().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test3JoinFlat()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4Join") {
      val gen = GeneratedCodeTests.Test4Join().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4Join()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("Test4JoinFlat") {
      val gen = GeneratedCodeTests.Test4JoinFlat().drop("Region_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.Test4JoinFlat()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("TestFN0") {
      val gen = GeneratedCodeTests.TestFN0().toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.TestFN0()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("TestFN1") {
      val gen = GeneratedCodeTests.TestFN1().toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.TestFN1()

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("TestFN2") {
      val gen = GeneratedCodeTests.TestFN2().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.TestFN2()

      gen.show(false)
      lib.show(false)

      assertDataFramesAreEquivalent(gen, lib)
    }

    it("TestFN2Full") {
      val gen = GeneratedCodeTests.TestFN2Full().drop("Nation_index").toDF()
      Symbol.freshClear()
      val lib = LibraryFlatToNestedTests.TestFN2Full()

      assertDataFramesAreEquivalent(gen, lib)
    }
  }
}
