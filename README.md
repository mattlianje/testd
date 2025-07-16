<p align="center">
  <img src="pix/testd.png" width="700">
</p>

# <img src="pix/testd-logo.png" width="60">   testd
**Pretty, flexible data-as-code**

Tabular data fixtures as simple, spreadsheet-like, executable code.
Part of [d4](https://github.com/mattlianje/d4)

## Features
- Spreadsheet-like data-as-code 🍰
- Reusable, composable test fixtures
- Works beautifully in REPL
- Drop **TestD.scala** in like a header file
- Easily extensible with your own algebras
- Batteries included Spark support (see Spark Integration)


## FAQ

**Q: Why TestD?**
- Because test data is critical - and painful to maintain in brittle case classes, or junk files outside your code.

**Q: Sure, but why?**
- Because **TestD** lets you define tabular data once, then reuse & compose it across systems. (Spark is just one target),
plug in your own algebras and extension methods.

**Q: Why a spreadsheet format?**
- Because it’s visual, writable by humans, easy to diff and battle-tested for centuries (even if derided in some CS circles).

**Q: Why not use Scala literals, DataFrame code, or generators?**
- Because they’re noisy, and tied to structure. **TestD** is readable, and delays schema application until you need it. The inspiration
here is the Clojure/Lisp-y code-as-data way.

**Q: Who is this for?**
- Programmers tired of rotting `.csv`, `.json`, or `.txt` fixtures in their `resources/` or having to unbreak their test objects
everytime their data models evolve.

## Table of Contents
- [Features](#features)
- [FAQ](#faq)
- [Get Started](#get-started)
- [Basic API](#basic-api)
  - [TestD](#testd)
  - [toMap](#toMap)
- [Editing](#editing)
- [Composition](#composition)
- [Spark](#spark)
  - [Conversions](#conversions)
    - [toDf](#toDf)
    - [fromDf](#fromdf)
  - [Casting](#casting)
    - [castToSchema](#casttochema)
    - [conformToSchema](#conformtoschema)
    - [filterToSchema](#filtertochema)
    - [Nested data](#nested-data)
- [More examples](#more-examples)


## Get started
> [!WARNING]  
> TestD API is still undergoing tweaks. Breaking changes might happen

Load the latest `master` in your spark-shell:
```bash
spark-shell -i <(curl -sL https://raw.githubusercontent.com/mattlianje/testd/master/TestD.scala)
```

All you need is `import testd._` and start creating beautiful data:
```scala
import testd._

scala> val people = TestD(
     |   ("name", "age", "city"),
     |   ("Alice", 25, "New York"),
     |   ("Bob", 30, "London")
     | )
people: testd.TestD[(String, Any, String)] =
TestD(
  ("NAME" , "AGE", "CITY"    ),
  ("Alice", 25   , "New York"),
  ("Bob"  , 30   , "London"  )
)
```

## Basic API
A TestD is just a case class that wraps some a `Seq` of `Map[String, Any]`
### TestD
TestD supports multiple construction patterns:

**From tuples (first row = headers):**
```scala
val data = TestD(
  ("id", "score", "grade"),
  (1, 95, "A"),
  (2, 87, "B")
)
```

**From sequences:**
```scala
val data = TestD(
  Seq("id", "score", "grade"),
  Seq(1, 95, "A"),
  Seq(2, 87, "B")
)
```

**From Maps (no separate headers):**

You can create a **TestD** from a Map and vice-versa.
**Why is this useful?** Because a `Seq[Map[String, Any]]` is structurally isomorphic to a spreadsheet:
```scala
val data = TestD(
  Map("id" -> 1, "score" -> 95, "grade" -> "A"),
  Map("id" -> 2, "score" -> 87, "grade" -> "B")
)
```

**`.asMaps`**
Get consistent Map representation regardless of construction method:
```scala
val tupleData = TestD(("name", "age"), ("Alice", 25))
val maps: Seq[Map[String, Any]] = tupleData.asMaps
// Seq(Map("NAME" -> "Alice", "AGE" -> 25))
```

Maps are everywhere: logs, JSON, APIs - and **TestD** gives them shape, order, and schema control.

## Editing
You can edit **TestD**'s like mini dataframes

This lets you quickly re-jig them in your REPL or stack modifications on top of base **TestD**'s

### `.withColumn`
Add columns to existing TestD:
```scala
val people = TestD(("name", "age"), ("Alice", 25))

// Add column with default value
val withCountry = people.withColumn("country", "USA")

// Add column with null
val withPhone = people.withColumn("phone")
```

### `.select`
Choose specific columns:
```scala
val data = TestD(
  ("name", "age", "city", "country"),
  ("Alice", 25, "NYC", "USA")
)

val subset = data.select("name", "age")
// Only NAME and AGE columns
```

### `.drop`
Remove unwanted columns:
```scala
val cleaned = data.drop("country", "city")
// Everything except COUNTRY and CITY
```

## Composition
You can also compose **TestD**'s

These composition functions work on a row-by-row basis, comparing all column values to determine matches.

### `.union`
Combine **TestD** instances:
```scala
val team1 = TestD(("name", "role"), ("Alice", "Dev"))
val team2 = TestD(("name", "role"), ("Bob", "PM"))

val allTeam = team1.union(team2)
// Contains all rows from both teams
```

### `.intersect`
Find common rows:
```scala
val currentUsers = TestD(("id", "name"), (1, "Alice"))
val activeUsers = TestD(("id", "name"), (1, "Alice"), (2, "Bob"))

val overlap = currentUsers.intersect(activeUsers)
// Rows that exist in both
```

### `.except`
Set difference:
```scala
val allUsers = TestD(("id", "name"), (1, "Alice"), (2, "Bob"))
val inactiveUsers = TestD(("id", "name"), (2, "Bob"))

val activeOnly = allUsers.except(inactiveUsers)
// Rows in allUsers but not in inactiveUsers
```

### `.contains`
Check subset relationships:
```scala
val subset = TestD(("name", "age"), ("Alice", 25))
val superset = TestD(("name", "age"), ("Alice", 25), ("Bob", 30))

superset.contains(subset) /* true */
subset.contains(superset) /* false */
```

## Spark
**TestD** comes packaged with spark helpers.

### Conversions
This lets your slurp any Spark DataFrame into a **TestD** ...

and vice-versa convert any **TestD** into a DataFrame.

#### `.toDf`
Convert **TestD** to Spark DataFrame:
```scala
val testd = TestD(("name", "age"), ("Alice", 25))
val df = testd.toDf(spark)
// DataFrame with StringType columns
```

#### `.fromDf`
Create **TestD** from DataFrame (preserves complex types as JSON):
```scala
val df = spark.createDataFrame(...)
val testd = TestD.fromDf(df)
// Complex nested structures become JSON strings
```

### Casting
These 3 methods make it trivial to cast your **TestD** into any DataFrame with pretty types.
#### `.castToSchema`
Cast DataFrame columns to match schema types:
```scala
val df = testd.toDf(spark) // All StringType
val schema = StructType(Seq(
  StructField("NAME", StringType),
  StructField("AGE", IntegerType)
))

val typed = TestD.castToSchema(df, schema)
// AGE column now IntegerType
```

#### `.conformToSchema`
Ensure DataFrame matches schema exactly (adds missing columns as nulls):
```scala
val df = spark.createDataFrame(Seq(("Alice",))).toDF("name")
val schema = StructType(Seq(
  StructField("name", StringType),
  StructField("age", IntegerType)
))

val conformed = TestD.conformToSchema(df, schema)
// Now has both name and age columns
```

#### `.filterToSchema`
Keep only columns that exist in target schema:
```scala
val df = spark.createDataFrame(Seq(("Alice", 25, "extra")))
  .toDF("name", "age", "unwanted")
val schema = StructType(Seq(
  StructField("name", StringType),
  StructField("age", IntegerType)
))

val filtered = TestD.filterToSchema(df, schema)
// "unwanted" column removed, types cast to schema
```

#### Nested data
**TestD** handles complex nested structures by converting them to JSON strings:

```scala
import testd._
import org.apache.spark.sql.types._

/* You have the REPRESENTATION of some test data */
val complexData = TestD(
  ("id", "profile", "scores"),
  (1, """{"name": "Alice", "meta": {"joined": "2023"}}""", "[95, 87, 92]")
)

/* You have your nice types */
val nestedSchema = StructType(Seq(
  StructField("ID", IntegerType),
  StructField("PROFILE", StructType(Seq(
    StructField("name", StringType),
    StructField("meta", StructType(Seq(
      StructField("joined", StringType)
    )))
  ))),
  StructField("SCORES", ArrayType(IntegerType))
))

/* Transforms you TestD into a DataFrame with nice types */
val df = complexData.toDf(spark)
val typedDf = TestD.castToSchema(df, nestedSchema)

/* Verify the schema works */
typedDf.select("PROFILE.name", "SCORES").show()
```
You will see:
```
 +-----+------------+
 | name|      SCORES|
 +-----+------------+
 |Alice|[95, 87, 92]|
 +-----+------------+
```

Then you can yank your spark df back into a TestD fixture
```scala
val backToTestD = TestD.fromDf(typedDf)
println(backToTestD)
```
You will see:
```scala
TestD(
  ("ID", "PROFILE"                                      , "SCORES"        ),
  ("1" , """{"name":"Alice","meta":{"joined":"2023"}}""", """[95,87,92]""")
)
```


## More examples
- Generate nested data and avoid boilerplate with Scala collections
```scala
val products = for {
  category <- List("Electronics", "Books", "Games")
  id <- 1 to 3
  price = id * 10.99
  inStock = id % 2 == 0
} yield (s"$category$id", category, price, inStock)

val productData = TestD(("product", "category", "price", "available") +: products)
println(productData)

/*
TestD(
  ("PRODUCT"     , "CATEGORY"   , "PRICE", "AVAILABLE"),
  ("Electronics1", "Electronics", 10.99  , false      ),
  ("Electronics2", "Electronics", 21.98  , true       ),
  ("Electronics3", "Electronics", 32.97  , false      ),
  ("Books1"      , "Books"      , 10.99  , false      ),
  ("Books2"      , "Books"      , 21.98  , true       ),
  ("Books3"      , "Books"      , 32.97  , false      ),
  ("Games1"      , "Games"      , 10.99  , false      ),
  ("Games2"      , "Games"      , 21.98  , true       ),
  ("Games3"      , "Games"      , 32.97  , false      )
)
*/
```
