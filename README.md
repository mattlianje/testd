<p align="center">
  <img src="pix/testd.png" width="700">
</p>

# <img src="pix/testd-logo.png" width="60">   testd _(𝛼)_
**Pretty, flexible, simple fixtures**

A lightweight Scala [quoted-DSL](https://homepages.inf.ed.ac.uk/wadler/papers/qdsl/qdsl.pdf) to represent your test fixtures as beautiful,
easy to edit, executable code. Part of [d4s](https://github.com/mattlianje/d4s)

## Features
- Turns messy data -> ✨🍰 pretty, spreadsheet-like data-as-code
- Drop **TestD.scala** in any Spark project like a header file
- Lets Spark casting do the heavy lifting
- Move from REPL to unit tests for TDD style ETL

## Table of Contents
- [Features](#features)
- [Get Started](#get-started)
- [Of note ...](#of-note-)
- [Schema Operations](#schema-operations)
  - [castToSchema](#casttochema)
  - [conformToSchema](#conformtoschema)
  - [filterToSchema](#filtertochema)
- [Column Operations](#column-operations)
- [Creating nested data](#creating-nested-data)
- [More examples](#more-examples)


## Get started
> [!WARNING]  
> Releases sub `1.0.0` are experimental. Breaking API changes might happen

Want to try? **TestD** is on the Maven Central repo [here](https://central.sonatype.com/artifact/io.github.mattlianje/testd_2.13). Add it to your library dependencies:
```scala
"xyz.matthieucourt" % "testd_2.13" % "0.1.2"
```

Using scala-cli
```
scala-cli repl --dep org.apache.spark::spark-sql:3.5.0 \
               --dep xyz.matthieucourt::testd_2.13:0.1.2
```
Or load the latest `master` in your spark-shell:
```bash
spark-shell -i <(curl -sL https://raw.githubusercontent.com/mattlianje/testd/master/TestD.scala)
```

You just need to know 4 things:
1. The first row of a **TestD** is for column names
```scala
import testd._

val data = TestD(Seq(
  ("order_id", "customer", "items", "total", "priority", "delivered"),  /* Column names */
  ("A101", "Napac", 5, 299.99, "HIGH", true),                           /* Data rows... */
  ("B202", "Air Liquide", 1, 499.50, "LOW", false),
  ("C303", "A long company name", 3, 799.99, "HIGH", true)
))
```
2. Call `toDf`on a **TestD** to get a Spark DataFrame
```scala
val df = data.toDf(spark)
```
3. Call `println` on a **TestD** to get a ✨🍰 pretty **TestD**
```scala
println(data)                    
/*
TestD(Seq(
     ("ORDER_ID", "CUSTOMER"           , "ITEMS", "TOTAL", "PRIORITY", "DELIVERED"),
     ("A101"    , "Napac"              , 5      , 299.99 , "HIGH"    , true       ),
     ("B202"    , "Air Liqide"         , 1      , 499.50 , "LOW"     , false      ),
     ("C303"    , "A long company name", 3      , 799.99 , "HIGH"    , true       )
   ))
*/
```
4. Use the 3 **TestD** schema operations below.

## Of note ...
- Ultimately **TestD** is a dead-simple little DSL, but it puts forward a new "code is the data" approach.
- The spreadsheet format is sometimes derided in CS circles, but has been battle-tested and loved for centuries.
- The idea is for your fixtures to be "representations" of your data and not materialized instances already bound to a target schema.
- ^ This is why **TestD** has some built-in niceties for casting, filtering, and conforming the representations of your dataframes.

## Schema Operations
There are 3 building blocks with **TestD** casting: `castToSchema`, `conformToSchema`, `filterToSchema`

Imagine ...
- We create a quick and messy DataFrame:
```scala
val messyDf = spark.createDataFrame(Seq(
  ("1", "2023-01-01", "99.9", "true"),
  ("2", "20230102", "88.8", "1"),
  ("3", "2023/01/03", "77.7", "FALSE")
)).toDF("ID", "DATE", "AMOUNT", "ACTIVE")
```

- But, like often, we have a target schema with nice types:
```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("id", IntegerType),
  StructField("date", DateType),
  StructField("amount", DoubleType),
  StructField("active", BooleanType),
  StructField("category", StringType)
))
```

#### `castToSchema`
   
Cast DataFrame columns if they exist in schema, preserves structure:
```scala
val castedDf = TestD.castToSchema(messyDf, schema)
castedDf.show()
```
```
+---+----------+------+-------+
| ID|      DATE|AMOUNT|ACTIVE |
+---+----------+------+-------+
|  1|2023-01-01|  99.9|  true |
|  2|2023-01-02|  88.8|  true |
|  3|2023-01-03|  77.7| false |
+---+----------+------+-------+
```

#### `conformToSchema`
   
Makes DataFrame match exactly - handles missing/extra columns:
```scala
val conformedDf = TestD.conformToSchema(messyDf, schema)
conformedDf.show()
```
```
+---+----------+------+-------+--------+
| id|      date|amount|active |category|
+---+----------+------+-------+--------+
|  1|2023-01-01|  99.9|  true |    null|
|  2|2023-01-02|  88.8|  true |    null|
|  3|2023-01-03|  77.7| false |    null|
+---+----------+------+-------+--------+
```

#### `filterToSchema`
   
Keeps only DataDrame columns if names exist in schema

```scala
val extraDf = messyDf.withColumn("EXTRA", lit("unwanted"))
val filteredDf = TestD.filterToSchema(extraDf, schema)
filteredDf.show()
```
```
+---+----------+------+-------+
| id|      date|amount|active |
+---+----------+------+-------+
|  1|2023-01-01|  99.9|  true |
|  2|2023-01-02|  88.8|  true |
|  3|2023-01-03|  77.7| false |
+---+----------+------+-------+
```

#### `fromDf`
   
Convert a Spark DataFrame back to TestD:
```scala
val df = spark.createDataFrame(Seq(
  (1, "Alice"), 
  (2, "Bob")
)).toDF("id", "name")

val testd = TestD.fromDf(df)
println(testd)
/*
TestD(Seq(
  ("ID"  , "NAME" ),
  ("1"   , "Alice"),
  ("2"   , "Bob"  )
))
*/
```

## Column Operations
**TestD** makes it dead simple to manipulate your test data, just like you would with a Spark DataFrame

Start with some sample data:
```scala
import testd._

val orders = TestD(Seq(
  ("order_id", "customer", "items", "total", "priority"),
  ("A101", "Acme Corp", 5, 299.99, "HIGH"),
  ("B202", "Globex", 1, 499.50, "LOW"),
  ("C303", "Initech", 3, 799.99, "HIGH")
))
```

Add a new column with a default value:
```scala
val withStatus = orders.withColumn("status", "pending")
println(withStatus)
/*
TestD(Seq(
  ("ORDER_ID", "CUSTOMER", "ITEMS", "TOTAL" , "PRIORITY", "STATUS" ),
  ("A101"    , "Acme Corp", 5     , 299.99  , "HIGH"    , "pending"),
  ("B202"    , "Globex"   , 1     , 499.50  , "LOW"     , "pending"),
  ("C303"    , "Initech"  , 3     , 799.99  , "HIGH"    , "pending")
))
*/
```

Or add a column with null values for later population
```scala
val withNotes = orders.withColumn("notes")
println(withNotes)
/*
TestD(Seq(
  ("ORDER_ID", "CUSTOMER", "ITEMS", "TOTAL" , "PRIORITY", "NOTES"),
  ("A101"    , "Acme Corp", 5     , 299.99  , "HIGH"    , null  ),
  ("B202"    , "Globex"   , 1     , 499.50  , "LOW"     , null  ),
  ("C303"    , "Initech"  , 3     , 799.99  , "HIGH"    , null  )
))
*/
```

Select only the columns you need
```scala
val essential = orders.select("order_id", "customer", "total")
println(essential)
/*
TestD(Seq(
  ("ORDER_ID", "CUSTOMER", "TOTAL" ),
  ("A101"    , "Acme Corp", 299.99 ),
  ("B202"    , "Globex"   , 499.50 ),
  ("C303"    , "Initech"  , 799.99 )
))
*/
```

Drop columns you don't want
```scala
val simplified = orders.drop("priority")
println(simplified)
/*
TestD(Seq(
  ("ORDER_ID", "CUSTOMER", "ITEMS", "TOTAL" ),
  ("A101"    , "Acme Corp", 5     , 299.99  ),
  ("B202"    , "Globex"   , 1     , 499.50  ),
  ("C303"    , "Initech"  , 3     , 799.99  )
))
*/
```

Convert to a Map representation for comparison or serialization
```scala
val mapRepresentation = orders.toMap
println(mapRepresentation)
/*
TestD(Seq(
  Map("order_id" -> "A101", "customer" -> "Acme Corp", "items" -> 5, "total" -> 299.99, "priority" -> "HIGH"),
  Map("order_id" -> "B202", "customer" -> "Globex", "items" -> 1, "total" -> 499.50, "priority" -> "LOW"),
  Map("order_id" -> "C303", "customer" -> "Initech", "items" -> 3, "total" -> 799.99, "priority" -> "HIGH")
))
*/
```

## Creating nested data
```scala
import testd._
import org.apache.spark.sql.types._

val studentSchema = StructType(Seq(
 StructField("id", IntegerType),
 StructField("student", StructType(Seq(
   StructField("name", StringType),
   StructField("grades", ArrayType(IntegerType)),
   StructField("subjects", ArrayType(StringType))
 )))
))

val data = TestD(Seq(
 ("id", "student"),
 (1, """{
   "name": "Alice", 
   "grades": [95, 87, 92],
   "subjects": ["math", "english"]
 }"""),
 (2, """{
   "name": "Bob",
   "grades": [82, 85, 88], 
   "subjects": ["math", "history"]  
 }""")
))

val df = TestD.conformToSchema(data.toDf(spark), studentSchema)
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
TestD(Seq(
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
))
*/
```

- Typical messy data - hard to read, inconsistent formatting:
```scala
val messyData = spark.createDataFrame(Seq(
  (null, "JOHN.DOE", "10,000.50", "20230101", "SALES", "YES"),
  ("A12345", "Louis XI the Universal Spider", "8,500.00", null, "MARKETING", "1"),
  ("B78901", "Bob Wilson Jr", "12500", "2023/03", "sales", "NO")
)).toDF("ID", "name", "SALARY", "START_DT", "Department", "ACTIVE")
```
- ✨🍰 Pretty **TestD** data
```scala
TestD(Seq(
  ("ID"    , "NAME"                          , "SALARY" , "START_DATE", "DEPARTMENT", "ACTIVE"),
  ("A12345", "Louis XI the Universal Spider" , 8500.00  , null        , "Marketing" , true    ),
  ("B78901", "Bob Wilson"                    , 12500.00 , "2023-03-01", "Sales"     , false   ),
  (null    , "John Doe"                      , 10000.50 , "2023-01-01", "Sales"     , true    )
))
```
