
class TestDataTest extends munit.FunSuite {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row}

  test("TestData should handle tuples with header as first row and commented separator") {
    val data = TestData(Seq(
      ("name", "age"),
      ("Alice", 25),
      ("Bob", 30)
    ))
    
    assertEquals(
      data.toString,
      """|TestData(Seq(
         |  ("NAME" , "AGE"),
         |  ("Alice", 25   ),
         |  ("Bob"  , 30   )
         |))""".stripMargin
    )

    println(data.toString)
    
    assertEquals(data.headers, Seq("NAME", "AGE"))
    assertEquals(data.rows, Seq(
      Seq("Alice", 25),
      Seq("Bob", 30)
    ))
  }
  
  test("TestData should handle sequences with header as first row and commented separator") {
    val data = TestData(Seq(
      Seq("name", "age", "city"),
      Seq("Alice", 25, "New York"),
      Seq("Bob", 300000, "London")
    ))
    
    assertEquals(
      data.toString,
      """|TestData(Seq(
         |  Seq("NAME" , "AGE" , "CITY"    ),
         |  Seq("Alice", 25    , "New York"),
         |  Seq("Bob"  , 300000, "London"  )
         |))""".stripMargin
    )

    println(data.toString)
    
    assertEquals(data.headers, Seq("NAME", "AGE", "CITY"))
    assertEquals(data.rows, Seq(
      Seq("Alice", 25, "New York"),
      Seq("Bob", 300000, "London")
    ))
  }

  lazy val spark = SparkSession
    .builder()
    .appName("TestDataTest")
    .master("local[*]")
    .getOrCreate()
  
  test("TestData should handle tuples with uppercase headers") {
    val data = TestData(Seq(
      ("name", "age"),
      ("Alice", 25),
      ("Bob", 30)
    ))
    
    assertEquals(
      data.toString,
      """|TestData(Seq(
         |  ("NAME" , "AGE"),
         |  ("Alice", 25   ),
         |  ("Bob"  , 30   )
         |))""".stripMargin
    )
    
    assertEquals(data.headers, Seq("NAME", "AGE"))
    assertEquals(data.rows, Seq(
      Seq("Alice", 25),
      Seq("Bob", 30)
    ))
    
    val df = data.toDf(spark)
    assertEquals(df.columns.toSeq, Seq("NAME", "AGE"))
    assertEquals(df.count(), 2L)
  }
  
  test("TestData should handle sequences with uppercase headers") {
    val data = TestData(Seq(
      Seq("name", "age", "city"),
      Seq("Alice", 25, "New York"),
      Seq("Bob", 30, "London")
    ))
    
    assertEquals(
      data.toString,
      """|TestData(Seq(
         |  Seq("NAME" , "AGE", "CITY"    ),
         |  Seq("Alice", 25   , "New York"),
         |  Seq("Bob"  , 30   , "London"  )
         |))""".stripMargin
    )
    
    assertEquals(data.headers, Seq("NAME", "AGE", "CITY"))
    assertEquals(data.rows, Seq(
      Seq("Alice", 25, "New York"),
      Seq("Bob", 30, "London")
    ))
    
    val df = data.toDf(spark)
    assertEquals(df.columns.toSeq, Seq("NAME", "AGE", "CITY"))
    assertEquals(df.count(), 2L)
  }
}
