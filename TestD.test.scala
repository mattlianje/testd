package testd

class TestDTest extends munit.FunSuite {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

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
         |  ("NAME" , "AGE" , "CITY"    ),
         |  ("Alice", 25    , "New York"),
         |  ("Bob"  , 300000, "London"  )
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
         |  ("NAME" , "AGE", "CITY"    ),
         |  ("Alice", 25   , "New York"),
         |  ("Bob"  , 30   , "London"  )
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

    val castedDf = TestD.castToSchema(inputDf, schema)
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

  test("schema functions should respect case sensitivity option") {
    val inputData = Seq(
      ("Alice", 25, "NY", 100),
      ("Bob", 30, "LA", 200)
    )
    val inputDf = spark
      .createDataFrame(inputData)
      .toDF("Name", "AGE", "city", "VALUE")

    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("Age", IntegerType),
        StructField("CITY", StringType)
      )
    )

    val castedDfSensitive =
      TestD.castToSchema(inputDf, schema, caseSensitive = true)
    assertEquals(
      castedDfSensitive.columns.toSet,
      Set("Name", "AGE", "city", "VALUE")
    )
    assertEquals(
      castedDfSensitive.schema.fields.find(_.name == "Name").get.dataType,
      StringType
    )

    val castedDfInsensitive = TestD.castToSchema(inputDf, schema)
    assertEquals(
      castedDfInsensitive.schema.fields.find(_.name == "Name").get.dataType,
      StringType
    )

    val conformedDfSensitive =
      TestD.conformToSchema(inputDf, schema, caseSensitive = true)
    assertEquals(
      conformedDfSensitive.columns.toSet,
      schema.fields.map(_.name).toSet
    )
    assert(conformedDfSensitive.filter(col("name").isNotNull).count() == 0)

    val filteredDfSensitive =
      TestD.filterToSchema(inputDf, schema, caseSensitive = true)
    assertEquals(filteredDfSensitive.columns.length, 0)

    val filteredDfInsensitive = TestD.filterToSchema(inputDf, schema)
    assertEquals(filteredDfInsensitive.columns.length, 3)
  }

  test("TestD should handle complex types using castToSchema") {
    val data = TestD(
      Seq(
        ("id", "scores", "details"),
        (1, "[95,87,92]", """{"name":"Alice","grade":"A"}"""),
        (2, "[88,85,90]", """{"name":"Bob","grade":"B"}""")
      )
    )

    println(data)
    val df = data.toDf(spark)

    val targetSchema = StructType(
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

    val finalDf = TestD.castToSchema(df, targetSchema)

    assertEquals(finalDf.schema.fields(1).dataType, ArrayType(IntegerType))
    assert(finalDf.schema.fields(2).dataType.isInstanceOf[StructType])
  }

  test("TestD should handle deeply nested complex types") {
    val data = TestD(
      Seq(
        ("id", "student_record"),
        (
          1,
          """{
           "name": "Alice",
           "grades": {
             "subjects": {
               "math": {"scores": [95, 87, 92], "teacher": "Smith"},
               "english": {"scores": [88, 91, 85], "teacher": "Jones"}
             },
             "overall": "A"
           },
           "activities": [
             {"name": "chess", "level": "advanced"},
             {"name": "debate", "level": "intermediate"}
           ],
           "metadata": {
             "enrollmentDate": "2023-01",
             "tags": ["honors", "stem"]
           }
         }"""
        ),
        (
          2,
          """{
           "name": "Bob",
           "grades": {
             "subjects": {
               "math": {"scores": [82, 85, 88], "teacher": "Smith"},
               "english": {"scores": [90, 92, 87], "teacher": "Jones"}
             },
             "overall": "B"
           },
           "activities": [
             {"name": "soccer", "level": "advanced"}
           ],
           "metadata": {
             "enrollmentDate": "2023-01",
             "tags": ["sports"]
           }
         }"""
        )
      )
    )

    val df = data.toDf(spark)

    val activityType = StructType(
      Seq(
        StructField("name", StringType),
        StructField("level", StringType)
      )
    )

    val subjectType = StructType(
      Seq(
        StructField("scores", ArrayType(IntegerType)),
        StructField("teacher", StringType)
      )
    )

    val gradesType = StructType(
      Seq(
        StructField("subjects", MapType(StringType, subjectType)),
        StructField("overall", StringType)
      )
    )

    val metadataType = StructType(
      Seq(
        StructField("enrollmentDate", StringType),
        StructField("tags", ArrayType(StringType))
      )
    )

    val studentRecordType = StructType(
      Seq(
        StructField("name", StringType),
        StructField("grades", gradesType),
        StructField("activities", ArrayType(activityType)),
        StructField("metadata", metadataType)
      )
    )

    val targetSchema = StructType(
      Seq(
        StructField("ID", IntegerType),
        StructField("STUDENT_RECORD", studentRecordType)
      )
    )

    val finalDf = TestD.castToSchema(df, targetSchema)

    assertEquals(finalDf.schema.fields(0).dataType, IntegerType)

    val recordType = finalDf.schema.fields(1).dataType.asInstanceOf[StructType]
    assertEquals(recordType.fields(0).name, "name")
    assertEquals(recordType.fields(1).name, "grades")

    val gradesField = recordType.fields(1).dataType.asInstanceOf[StructType]
    assert(gradesField.fields(0).dataType.isInstanceOf[MapType])

    val row = finalDf.where(col("ID") === 1).select("STUDENT_RECORD").first()
    val record = row.getStruct(0)

    assertEquals(record.getString(0), "Alice")

    val grades = record.getStruct(1)
    val subjects = grades.getMap[String, Row](0)
    val mathScores = subjects("math").getSeq[Int](0)
    assertEquals(mathScores, Seq(95, 87, 92))

    val activities = record.getSeq[Row](2)
    assertEquals(activities(0).getString(0), "chess")
    assertEquals(activities(0).getString(1), "advanced")

    val metadata = record.getStruct(3)
    assertEquals(metadata.getSeq[String](1), Seq("honors", "stem"))
  }

  test("TestD should handle Maps with different keys") {
    val data = TestD(
      Seq(
        Map("name" -> "Alice", "age" -> 25),
        Map("name" -> "Bob", "city" -> "NY"),
        Map("age" -> 30, "active" -> true)
      )
    )

    assertEquals(data.headers, Seq("ACTIVE", "AGE", "CITY", "NAME"))

    assertEquals(
      data.rows,
      Seq(
        Seq(null, 25, null, "Alice"),
        Seq(null, null, "NY", "Bob"),
        Seq(true, 30, null, null)
      )
    )

    assertEquals(
      data.toString,
      """|TestD(Seq(
       |  ("ACTIVE", "AGE", "CITY", "NAME" ),
       |  (null    , 25   , null  , "Alice"),
       |  (null    , null , "NY"  , "Bob"  ),
       |  (true    , 30   , null  , null   )
       |))""".stripMargin
    )

    assertEquals(
      data.toMap,
      """|TestD(Seq(
       |  Map("age" -> 25, "name" -> "Alice"),
       |  Map("city" -> "NY", "name" -> "Bob"),
       |  Map("active" -> true, "age" -> 30)
       |))""".stripMargin
    )
  }
  test("TestD should support adding and dropping columns") {
    val data = TestD(
      Seq(
        Map("name" -> "Alice", "age" -> 25),
        Map("name" -> "Bob", "age" -> 30)
      )
    )

    val withCity = data.withColumn("city")
    assertEquals(
      withCity.toMap,
      """|TestD(Seq(
       |  Map("age" -> 25, "city" -> null, "name" -> "Alice"),
       |  Map("age" -> 30, "city" -> null, "name" -> "Bob")
       |))""".stripMargin
    )

    val withCountry = data.withColumn("country", "USA")
    assertEquals(
      withCountry.toMap,
      """|TestD(Seq(
       |  Map("age" -> 25, "country" -> "USA", "name" -> "Alice"),
       |  Map("age" -> 30, "country" -> "USA", "name" -> "Bob")
       |))""".stripMargin
    )

    val droppedAge = withCountry.drop("age")
    assertEquals(
      droppedAge.toMap,
      """|TestD(Seq(
       |  Map("country" -> "USA", "name" -> "Alice"),
       |  Map("country" -> "USA", "name" -> "Bob")
       |))""".stripMargin
    )

    val droppedMultiple = withCountry.drop("age", "country")
    assertEquals(
      droppedMultiple.toMap,
      """|TestD(Seq(
       |  Map("name" -> "Alice"),
       |  Map("name" -> "Bob")
       |))""".stripMargin
    )
  }

  test("TestD should use Seq format when more than 22 columns") {
    val manyColumns = Seq.tabulate(23)(i => s"col$i")
    val data = TestD(
      Seq(
        manyColumns,
        manyColumns.map(_ => "value")
      )
    )

    val mapData = TestD(
      Seq(
        (1 to 23).map(i => s"col$i").map(col => col -> "value").toMap
      )
    )
    assert(mapData.headers.length > 22)
  }

  test("TestD should handle columns with nulls as StringType") {
    val data = TestD(
      Seq(
        Map("col1" -> "a", "col2" -> 1),
        Map("col1" -> null, "col2" -> 2),
        Map("col1" -> "c", "col2" -> null)
      )
    )

    val data2 = TestD(
      Seq(
        Map("foo" -> 1),
        Map("bar" -> 2)
      )
    )

    val df2 = data2.toDf(spark).show()

    val df = data.toDf(spark)
    assertEquals(df.schema("COL1").dataType, StringType)
    assertEquals(df.schema("COL2").dataType, StringType)
  }
}
