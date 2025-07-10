<p align="center">
  <img src="pix/testd.png" width="700">
</p>

# <img src="pix/testd-logo.png" width="60">   testd
**Pretty, flexible, simple fixtures**

A lightweight Scala DSL for expressing test fixtures as beautiful, editable, executable, and re-usable code.
Part of [d4](https://github.com/mattlianje/d4)

## Features
- TestD is a tiny algebra for tabular test data
- Spreadsheet-like data-as-code 🍰
- Re-usable, composable fixtures
- Ships with Spark integration (`.toDf`, `.fromDf`, schema helpers)
- Works beautifully in REPL
- Drop **TestD.scala** in like a header file
- Bring your own interpreter

## FAQ

**Why does this exist?**  
Because test data is critical — and painful to maintain in brittle case classes, external JSON, or scattered files.

**What’s the real point?**  
**TestD** is just an algebra. You define clean tabular data once and reuse it across systems. Spark is one target - not the core.

**Why a spreadsheet format?**  
Because it’s visual, writable by humans, easy to diff and battle-tested for centuries (even if derided in some CS circles).

**Why not use Scala literals, DataFrame code, or generators?**  
Because they’re noisy, and tied to structure. **TestD** is readable, and delays schema application until you need it. The inspiration here is the Clojure way.

**Who is this for?**  
Anyone tired of rotting `.csv`, `.json`, or `.txt` fixtures in your `resources/`.

## Table of Contents
- [Features](#features)
- [FAQ](#faq)
- [Get Started](#get-started)
- [Schema Operations](#schema-operations)
  - [castToSchema](#casttochema)
  - [conformToSchema](#conformtoschema)
  - [filterToSchema](#filtertochema)
  - [fromDf](#fromdf)
- [toMap](#tomap)
- [Column Operations](#column-operations)
- [Creating nested data](#creating-nested-data)
- [More examples](#more-examples)


## Get started
> [!WARNING]  
> TestD API is still undergoing tweaks. Breaking changes might happen

Load the latest `master` in your spark-shell:
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


## Schema Operations
TestD gives you three tools for aligning messy data to a target schema — depending on how strict or flexible you want to be.

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
**Gently cast what's there. Ignore the rest.**
Casts only the columns that exist in both the DataFrame and the schema. Leaves extra columns untouched.
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
**Force it to match exactly.**
Keeps only schema-defined columns. Adds nulls for missing ones. Drops any extras.
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
**Keep only what matches.**
Strips away all columns not present in the schema. No casting - just column selection.

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
   
Convert a Spark DataFrame back to **TestD**:
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

## `toMap` 
You can create a **TestD** from a Map and vice-versa.

**Why is this so useful?** Because a Seq[Map[String, Any]] is structurally isomorphic to a spreadsheet:

- Each Map = one row
- Keys = column names
- Missing keys? --> nulls (just like Excel)

```scala
val data = Seq(
  Map("id" -> 1, "name" -> "Alice"),
  Map("id" -> 2) /* no name */
)

val testd = TestD.fromMap(data)
println(testd)
/*
TestD(Seq(
  ("ID", "NAME" ),
  ("1",  "Alice"),
  ("2",  null   )
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
