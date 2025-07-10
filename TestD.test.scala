package testd

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

class TestDTest extends munit.FunSuite {

  lazy val spark =
    SparkSession.builder().appName("TestDTest").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val basicTuples = Seq(("name", "age"), ("Alice", 25), ("Bob", 30))
  val basicSeqs = Seq(
    Seq("name", "age", "city"),
    Seq("Alice", 25, "New York"),
    Seq("Bob", 300000, "London")
  )
  val mixedMaps = Seq(
    Map("name" -> "Alice", "age" -> 25),
    Map("name" -> "Bob", "city" -> "NY"),
    Map("age" -> 30, "active" -> true)
  )
  val jsonData = Seq(("id", "data"), (1, """{"x": 1}"""))

  val nestedSchema = StructType(
    Seq(
      StructField("id", IntegerType),
      StructField(
        "address",
        StructType(
          Seq(
            StructField("city", StringType),
            StructField("country", StringType)
          )
        )
      ),
      StructField("scores", ArrayType(IntegerType))
    )
  )

  test("basic formatting") {
    val tupleTestd = TestD(basicTuples)
    assertEquals(
      tupleTestd.toString,
      """|TestD(Seq(
         |  ("NAME" , "AGE"),
         |  ("Alice", 25   ),
         |  ("Bob"  , 30   )
         |))""".stripMargin
    )
    assertEquals(tupleTestd.headers, Seq("NAME", "AGE"))

    val seqTestd = TestD(basicSeqs)
    assertEquals(
      seqTestd.toString,
      """|TestD(Seq(
         |  ("NAME" , "AGE" , "CITY"    ),
         |  ("Alice", 25    , "New York"),
         |  ("Bob"  , 300000, "London"  )
         |))""".stripMargin
    )
  }

  test("map handling") {
    val testd = TestD(mixedMaps)
    assertEquals(testd.headers, Seq("ACTIVE", "AGE", "CITY", "NAME"))
    assertEquals(
      testd.toString,
      """|TestD(Seq(
         |  ("ACTIVE", "AGE", "CITY", "NAME" ),
         |  (null    , 25   , null  , "Alice"),
         |  (null    , null , "NY"  , "Bob"  ),
         |  (true    , 30   , null  , null   )
         |))""".stripMargin
    )

    val withCountry = testd.withColumn("country", "USA")
    val dropped = withCountry.drop("age")
    assertEquals(
      dropped.toMap,
      """|TestD(Seq(
         |  Map("country" -> "USA", "name" -> "Alice"),
         |  Map("city" -> "NY", "country" -> "USA", "name" -> "Bob"),
         |  Map("active" -> true, "country" -> "USA")
         |))""".stripMargin
    )
  }

  test("json formatting") {
    val testd = TestD(jsonData)
    assert(testd.toString.contains("\"\"\""))
    assert(testd.toMap.contains("\"\"\""))
  }

  test("schema operations") {
    val df = spark
      .createDataFrame(Seq(("Alice", "25", true, "extra")))
      .toDF("name", "age", "active", "extra")
    val schema = StructType(
      Seq(StructField("name", StringType), StructField("age", IntegerType))
    )

    val casted = TestD.castToSchema(df, schema)
    val conformed = TestD.conformToSchema(df, schema)
    val filtered = TestD.filterToSchema(df, schema)

    assertEquals(casted.columns.length, 4)
    assertEquals(conformed.columns.length, 2)
    assertEquals(filtered.columns.length, 2)
    assert(!filtered.columns.contains("extra"))
  }

  test("complex json casting") {
    val data = TestD(
      Seq(
        ("id", "scores", "details"),
        (1, "[95,87,92]", """{"name":"Alice","grade":"A"}""")
      )
    )
    val df = data.toDf(spark)
    val schema = StructType(
      Seq(
        StructField("ID", IntegerType),
        StructField("SCORES", ArrayType(IntegerType)),
        StructField(
          "DETAILS",
          StructType(
            Seq(
              StructField("name", StringType),
              StructField("grade", StringType)
            )
          )
        )
      )
    )
    val casted = TestD.castToSchema(df, schema)
    assertEquals(casted.schema.fields(1).dataType, ArrayType(IntegerType))
    assert(casted.schema.fields(2).dataType.isInstanceOf[StructType])
  }

  /** Testing .fromDf with nesting
    */
  test("nested structs round-trip") {
    val data = Seq(
      (1, Row("NY", "USA"), Seq(95, 87)),
      (2, Row("CA", "USA"), Seq(88, 85))
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data.map(Row.fromTuple)),
      nestedSchema
    )

    val testd = TestD.fromDf(df)
    assert(testd.toString.contains("\"city\":\"NY\""))
    assert(testd.toString.contains("[95,87]"))

    val recast = TestD.conformToSchema(testd.toDf(spark), nestedSchema)
    assertEquals(df.count(), recast.count())
  }

  /** Testing .fromDf with (deeper) nesting
    */
  test("deep nesting") {
    val deepSchema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField(
          "address",
          StructType(
            Seq(
              StructField(
                "location",
                StructType(
                  Seq(
                    StructField("city", StringType),
                    StructField("zipcode", StringType)
                  )
                )
              ),
              StructField("country", StringType)
            )
          )
        ),
        StructField(
          "grades",
          ArrayType(
            StructType(
              Seq(
                StructField("score", IntegerType),
                StructField("subject", StringType)
              )
            )
          )
        )
      )
    )

    val data = Seq((1, Row(Row("NY", "10001"), "USA"), Seq(Row(95, "Math"))))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data.map(Row.fromTuple)),
      deepSchema
    )
    val testd = TestD.fromDf(df)

    assert(testd.toString.contains("\"city\":\"NY\""))
    assert(testd.toString.contains("\"zipcode\":\"10001\""))
    assert(testd.toString.contains("\"subject\":\"Math\""))
  }

  test("edge cases") {
    /* Nulls */
    val nullTestd = TestD(Seq(Map("col1" -> "a", "col2" -> null)))
    assertEquals(nullTestd.toDf(spark).schema("COL2").dataType, StringType)

    /* Wide data */
    val wideMap = (1 to 25).map(i => s"col$i" -> i).toMap
    val wideTestd = TestD(Seq(wideMap))
    assertEquals(wideTestd.headers.length, 25)

    /* Case sensitivity */
    val df = spark.createDataFrame(Seq(("Alice", 25))).toDF("Name", "AGE")
    val schema = StructType(Seq(StructField("name", StringType)))
    assertEquals(
      TestD.filterToSchema(df, schema, caseSensitive = true).columns.length,
      0
    )
    assertEquals(TestD.filterToSchema(df, schema).columns.length, 1)
  }
}
