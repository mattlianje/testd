class TestDTest extends munit.FunSuite {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row}
  import org.apache.spark.sql.types._

  lazy val spark = SparkSession
    .builder()
    .appName("TestDTest")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test(
    "TestD should handle tuples with header as first row and commented separator"
  ) {
    val data = TestD(
      Seq(
        ("name", "age"),
        ("Alice", 25),
        ("Bob", 30)
      )
    )

    assertEquals(
      data.toString,
      """|TestD(Seq(
         |  ("NAME" , "AGE"),
         |  ("Alice", 25   ),
         |  ("Bob"  , 30   )
         |))""".stripMargin
    )

    println(data.toString)

    assertEquals(data.headers, Seq("NAME", "AGE"))
    assertEquals(
      data.rows,
      Seq(
        Seq("Alice", 25),
        Seq("Bob", 30)
      )
    )
  }

  test(
    "TestD should handle sequences with header as first row and commented separator"
  ) {
    val data = TestD(
      Seq(
        Seq("name", "age", "city"),
        Seq("Alice", 25, "New York"),
        Seq("Bob", 300000, "London")
      )
    )

    assertEquals(
      data.toString,
      """|TestD(Seq(
         |  Seq("NAME" , "AGE" , "CITY"    ),
         |  Seq("Alice", 25    , "New York"),
         |  Seq("Bob"  , 300000, "London"  )
         |))""".stripMargin
    )

    println(data.toString)

    assertEquals(data.headers, Seq("NAME", "AGE", "CITY"))
    assertEquals(
      data.rows,
      Seq(
        Seq("Alice", 25, "New York"),
        Seq("Bob", 300000, "London")
      )
    )
  }

  test("TestD should handle tuples with uppercase headers") {
    val data = TestD(
      Seq(
        ("name", "age"),
        ("Alice", 25),
        ("Bob", 30)
      )
    )

    assertEquals(
      data.toString,
      """|TestD(Seq(
         |  ("NAME" , "AGE"),
         |  ("Alice", 25   ),
         |  ("Bob"  , 30   )
         |))""".stripMargin
    )

    assertEquals(data.headers, Seq("NAME", "AGE"))
    assertEquals(
      data.rows,
      Seq(
        Seq("Alice", 25),
        Seq("Bob", 30)
      )
    )

    val df = data.toDf(spark)
    assertEquals(df.columns.toSeq, Seq("NAME", "AGE"))
    assertEquals(df.count(), 2L)
  }

  test("TestD should handle sequences with uppercase headers") {
    val data = TestD(
      Seq(
        Seq("name", "age", "city"),
        Seq("Alice", 25, "New York"),
        Seq("Bob", 30, "London")
      )
    )

    assertEquals(
      data.toString,
      """|TestD(Seq(
         |  Seq("NAME" , "AGE", "CITY"    ),
         |  Seq("Alice", 25   , "New York"),
         |  Seq("Bob"  , 30   , "London"  )
         |))""".stripMargin
    )

    assertEquals(data.headers, Seq("NAME", "AGE", "CITY"))
    assertEquals(
      data.rows,
      Seq(
        Seq("Alice", 25, "New York"),
        Seq("Bob", 30, "London")
      )
    )

    val df = data.toDf(spark)
    assertEquals(df.columns.toSeq, Seq("NAME", "AGE", "CITY"))
    assertEquals(df.count(), 2L)
  }

  test("schema functions should handle different column scenarios") {
    val inputData = Seq(
      ("Alice", "25", true, "extra1", 100),
      ("Bob", "30", false, "extra2", 200),
      ("Charlie", "35", true, "extra3", 300)
    )
    val inputDf = spark
      .createDataFrame(inputData)
      .toDF("NAME", "AGE", "ACTIVE", "EXTRA", "VALUE")

    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("rating", DoubleType),
        StructField("active", BooleanType)
      )
    )

    val castedDf = TestD.castMatchingColumns(inputDf, schema)
    assertEquals(castedDf.columns.length, 5)
    assertEquals(
      castedDf.schema.fields.find(_.name == "AGE").get.dataType,
      IntegerType
    )

    val conformedDf = TestD.conformToSchema(inputDf, schema)
    assertEquals(conformedDf.columns.toSet, schema.fields.map(_.name).toSet)
    assert(
      conformedDf.schema.fields.exists(f =>
        f.name == "rating" && f.dataType == DoubleType
      )
    )

    val filteredDf = TestD.filterToSchema(inputDf, schema)
    assert(!filteredDf.columns.contains("EXTRA"))
    assert(!filteredDf.columns.contains("VALUE"))
    assertEquals(
      filteredDf.schema.fields.map(_.name).toSet,
      Set("NAME", "AGE", "ACTIVE")
    )
  }
}
