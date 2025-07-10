/*
 * +==========================================================================+
 * |                                 testd                                    |
 * |           Tabular test data with precise formatting control              |
 * |                 Compatible with Spark 3.x and Scala 2.x                  |
 * |                                                                          |
 * | Copyright 2025 Matthieu Court (matthieu.court@protonmail.com)            |
 * | Apache License 2.0                                                       |
 * |                                                                          |
 * | Drop-in case class for creating aligned tabular test data with support   |
 * | for nested JSON, DataFrame round-trips, and schema operations            |
 * +==========================================================================+
 */
package testd

/** Tabular test data with precise formatting and DataFrame interop. Supports
  * tuples, sequences, and maps with automatic column alignment.
  */
case class TestD[T](data: Seq[T]) {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row}
  import org.apache.spark.sql.types._
  import scala.util.Try

  require(data.nonEmpty, "TestD must contain at least one row (headers)")
  require(
    data.forall(_.isInstanceOf[Map[_, _]]) || data.forall(
      !_.isInstanceOf[Map[_, _]]
    ),
    "TestD must contain either all maps or no maps"
  )

  /** Extract all unique keys from map data, sorted alphabetically */
  private def getAllHeaders: Seq[String] =
    data
      .flatMap {
        case map: Map[_, _] => map.keys
        case _              => Seq.empty
      }
      .distinct
      .map(_.toString.toUpperCase)
      .sorted

  /** Convert any row type to sequence for uniform processing */
  private def toSeq(row: Any): Seq[Any] = row match {
    case seq: Seq[_]    => seq
    case tuple: Product => tuple.productIterator.toSeq
    case map: Map[_, _] =>
      getAllHeaders.map(header =>
        map
          .asInstanceOf[Map[String, Any]]
          .find { case (k, _) => k.toUpperCase == header }
          .map(_._2)
          .getOrElse(null)
      )
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported row type: ${other.getClass}"
      )
  }

  val headers: Seq[Any] = if (data.head.isInstanceOf[Map[_, _]]) {
    getAllHeaders
  } else {
    toSeq(data.head).map {
      case s: String => s.toUpperCase
      case other     => other
    }
  }

  val rows: Seq[Seq[Any]] = if (data.head.isInstanceOf[Map[_, _]]) {
    data.map(toSeq)
  } else {
    data.tail.map(toSeq)
  }

  /** Generate map-style output with proper JSON escaping. Detects JSON strings
    * and wraps them in triple quotes.
    */
  def toMap: String = {
    val rowsAsMap = if (data.head.isInstanceOf[Map[_, _]]) {
      data.map(_.asInstanceOf[Map[String, Any]])
    } else {
      val columnNames = headers.map(_.toString)
      rows.map(row => columnNames.zip(row).toMap)
    }

    val formattedMaps = rowsAsMap
      .map(m =>
        s"  Map(${m.toSeq
            .sortBy(_._1)
            .map { case (k, v) =>
              v match {
                // JSON objects/arrays get triple-quoted
                case s: String if s.trim.matches("""^\s*\{[\s\S]*\}\s*$""") || s.trim.matches("""^\s*\[[\s\S]*\]\s*$""") =>
                  s""""$k" -> \"\"\"$s\"\"\""""
                case s: String => s""""$k" -> "$s""""
                case null      => s""""$k" -> null"""
                case other     => s""""$k" -> $other"""
              }
            }
            .mkString(", ")})"
      )
      .mkString(",\n")

    s"TestD(\n$formattedMaps\n)"
  }

  def toDf(spark: SparkSession): DataFrame = {
    val schema = headers.zipWithIndex.map { case (header, idx) =>
      StructField(header.toString, StringType, nullable = true)
    }

    val convertedRows = rows.map(row =>
      Row.fromSeq(row.map {
        case null => null
        case v    => v.toString
      })
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(convertedRows),
      StructType(schema)
    )
  }

  /** Generate aligned tabular output with automatic column width calculation.
    * JSON strings get triple quotes, everything else gets regular quotes.
    */
  override def toString: String = {
    val allRows = data.head match {
      case _: Map[_, _] => Seq(headers) ++ rows
      case _            => data.map(toSeq)
    }

    /* Calculate max width for each column including quote marks */
    val columnWidths = headers.indices.map { i =>
      allRows
        .map(row =>
          row(i) match {
            case s: String =>
              val str = if (row == headers) s.toUpperCase else s
              /* JSON gets triple quotes, adds extra length */
              if (
                str.trim.matches("""^\s*\{[\s\S]*\}\s*$""") || str.trim
                  .matches("""^\s*\[[\s\S]*\]\s*$""")
              ) {
                s"""\"\"\"$str\"\"\"""".length
              } else {
                s""""$str"""".length
              }
            case null  => "null".length
            case other => other.toString.length
          }
        )
        .max
    }

    /* Format a single row with proper padding and quote handling */
    def formatRow(row: Seq[Any], isHeader: Boolean = false): String = {
      row
        .zip(columnWidths)
        .map { case (value, width) =>
          value match {
            case s: String =>
              val str = if (isHeader) s.toUpperCase else s
              /* JSON strings get triple quotes for readability */
              if (
                str.trim.matches("""^\s*\{[\s\S]*\}\s*$""") || str.trim.matches(
                  """^\s*\[[\s\S]*\]\s*$"""
                )
              ) {
                s"""\"\"\"$str\"\"\"""".padTo(width, ' ')
              } else {
                s""""$str"""".padTo(width, ' ')
              }
            case null  => "null".padTo(width, ' ')
            case other => other.toString.padTo(width, ' ')
          }
        }
        .mkString(", ")
    }

    /* Use Seq() prefix for wide tables (>22 columns) */
    val useSeqPrefix = headers.length > 22
    val rowPrefix = if (useSeqPrefix) "  Seq(" else "  ("
    val headerFormatted = formatRow(headers, true)
    val formattedRows = rows.map(row => formatRow(row))

    s"""TestD(
      |$rowPrefix$headerFormatted),
      |${formattedRows.map(row => s"$rowPrefix$row)").mkString(",\n")}
      |)""".stripMargin
  }

  def withColumn(name: String): TestD[Map[String, Any]] =
    withColumn(name, null.asInstanceOf[String])

  def withColumn(name: String, value: Any): TestD[Map[String, Any]] = {
    new TestD[Map[String, Any]](
      if (data.head.isInstanceOf[Map[_, _]]) {
        data.map(_.asInstanceOf[Map[String, Any]] + (name -> value))
      } else {
        rows.map(row =>
          headers.map(_.toString).zip(row).toMap + (name -> value)
        )
      }
    )
  }

  def select(colNames: String*): TestD[Map[String, Any]] = {
    val upperColNames = colNames.map(_.toUpperCase)
    val nonExistentColumns = upperColNames.filterNot(col =>
      headers.map(_.toString.toUpperCase).contains(col)
    )
    require(
      nonExistentColumns.isEmpty,
      s"Columns not found: ${nonExistentColumns.mkString(", ")}"
    )

    new TestD[Map[String, Any]](
      if (data.head.isInstanceOf[Map[_, _]]) {
        data.map(_.asInstanceOf[Map[String, Any]].filter { case (k, _) =>
          upperColNames.contains(k.toUpperCase)
        })
      } else {
        val selectedIndices = upperColNames.map(col =>
          headers.map(_.toString.toUpperCase).indexOf(col)
        )
        rows.map(row =>
          selectedIndices
            .zip(upperColNames)
            .map { case (idx, colName) =>
              colName -> row(idx)
            }
            .toMap
        )
      }
    )
  }

  def drop(colNames: String*): TestD[Map[String, Any]] = {
    val upperColNames = colNames.map(_.toUpperCase)
    select(
      headers
        .map(_.toString)
        .filterNot(col => upperColNames.contains(col.toUpperCase)): _*
    )
  }

  /** Union two TestD instances, combining all rows from both. Columns are
    * aligned by name (case-insensitive), missing columns filled with null.
    * Result always uses Map representation for consistent column handling.
    */
  def union(other: TestD[_]): TestD[Map[String, Any]] = {
    // Get all unique column names from both TestDs
    val allColumns = (this.headers.map(_.toString.toUpperCase) ++
      other.headers.map(_.toString.toUpperCase)).distinct.sorted

    // Convert both TestDs to map representation
    val thisAsMap = if (this.data.head.isInstanceOf[Map[_, _]]) {
      this.data.map(_.asInstanceOf[Map[String, Any]])
    } else {
      this.rows.map(row => this.headers.map(_.toString).zip(row).toMap)
    }

    val otherAsMap = if (other.data.head.isInstanceOf[Map[_, _]]) {
      other.data.map(_.asInstanceOf[Map[String, Any]])
    } else {
      other.rows.map(row => other.headers.map(_.toString).zip(row).toMap)
    }

    // Normalize both datasets to have all columns
    def normalizeRow(row: Map[String, Any]): Map[String, Any] = {
      allColumns.map { col =>
        val value = row
          .find { case (k, _) => k.toUpperCase == col }
          .map(_._2)
          .getOrElse(null)
        col -> value
      }.toMap
    }

    val normalizedThis = thisAsMap.map(normalizeRow)
    val normalizedOther = otherAsMap.map(normalizeRow)

    new TestD(normalizedThis ++ normalizedOther)
  }

  /** Intersect two TestD instances, keeping only rows that exist in both. Rows
    * are compared by all column values (case-insensitive for column names).
    * Only columns present in BOTH TestDs are included in the result.
    */
  def intersect(other: TestD[_]): TestD[Map[String, Any]] = {
    // Find common columns (case-insensitive)
    val thisColumns = this.headers.map(_.toString.toUpperCase).toSet
    val otherColumns = other.headers.map(_.toString.toUpperCase).toSet
    val commonColumns = (thisColumns intersect otherColumns).toSeq.sorted

    require(
      commonColumns.nonEmpty,
      "Cannot intersect TestDs with no common columns"
    )

    val thisSelected = this.select(commonColumns: _*)
    val otherSelected = other.select(commonColumns: _*)

    // Convert to normalized map repr for comparisons
    val thisAsMap = thisSelected.data.map(_.asInstanceOf[Map[String, Any]])
    val otherAsMap = otherSelected.data.map(_.asInstanceOf[Map[String, Any]])

    // Normalized comparison keys
    def normalizeForComparison(row: Map[String, Any]): Map[String, String] = {
      row.map { case (k, v) =>
        k.toUpperCase -> (if (v == null) "NULL" else v.toString)
      }
    }

    val otherNormalized = otherAsMap.map(normalizeForComparison).toSet

    val intersection = thisAsMap.filter { row =>
      val normalized = normalizeForComparison(row)
      otherNormalized.contains(normalized)
    }

    new TestD(intersection)
  }

  /** Check if this TestD contains all rows from another TestD. Useful for
    * subset testing.
    */
  def contains(other: TestD[_]): Boolean = {
    import scala.util.{Try, Success, Failure}

    Try {
      val intersection = this.intersect(other)
      // Convert other to same column structure for comparison
      val otherNormalized =
        other.select(intersection.headers.map(_.toString): _*)
      intersection.data.size == otherNormalized.data.size
    }.recover { case _ =>
      false
    }.get
  }

  /** Remove rows that exist in another TestD (set difference). Only considers
    * columns present in BOTH TestDs for comparison.
    */
  def except(other: TestD[_]): TestD[Map[String, Any]] = {
    val intersection = this.intersect(other)
    val intersectionNormalized = intersection.data.map { row =>
      row.asInstanceOf[Map[String, Any]].map { case (k, v) =>
        k.toUpperCase -> (if (v == null) "NULL" else v.toString)
      }
    }.toSet

    // Convert this TestD to map
    val thisAsMap = if (this.data.head.isInstanceOf[Map[_, _]]) {
      this.data.map(_.asInstanceOf[Map[String, Any]])
    } else {
      this.rows.map(row => this.headers.map(_.toString).zip(row).toMap)
    }

    // Keep rows that don't match any in intersection
    val commonColumns = intersection.headers.map(_.toString.toUpperCase).toSet
    val result = thisAsMap.filter { row =>
      val relevantRow = row.filter { case (k, _) =>
        commonColumns.contains(k.toUpperCase)
      }
      val normalized = relevantRow.map { case (k, v) =>
        k.toUpperCase -> (if (v == null) "NULL" else v.toString)
      }
      !intersectionNormalized.contains(normalized)
    }

    new TestD(result)
  }
}

object TestD {
  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
  import scala.collection.mutable

  // def apply[T](data: Seq[T]): TestD[T] = new TestD(data)
  def apply[T](first: T, rest: T*): TestD[T] = new TestD(first +: rest)

  // Keep the Seq constructor for when you have a collection
  def apply[T](data: Seq[T]): TestD[T] = new TestD(data)

  // Convenience method for maps
  def maps(
      first: Map[String, Any],
      rest: Map[String, Any]*
  ): TestD[Map[String, Any]] =
    new TestD(first +: rest)

  private def normalizeColumnName(
      name: String,
      caseSensitive: Boolean
  ): String =
    if (caseSensitive) name else name.toUpperCase

  private def castColumn(colName: String, dataType: DataType): Column =
    dataType match {
      case _: ArrayType | _: StructType | _: MapType =>
        from_json(col(colName), dataType)
      case _ => col(colName).cast(dataType)
    }

  def castToSchema(
      df: DataFrame,
      schema: StructType,
      caseSensitive: Boolean = false
  ): DataFrame = {
    val schemaMap = schema.fields
      .map(f => normalizeColumnName(f.name, caseSensitive) -> f.dataType)
      .toMap
    val castColumns = df.columns.map { colName =>
      schemaMap
        .get(normalizeColumnName(colName, caseSensitive))
        .map(dataType => castColumn(colName, dataType))
        .getOrElse(col(colName))
        .as(colName)
    }
    df.select(castColumns: _*)
  }

  def conformToSchema(
      df: DataFrame,
      schema: StructType,
      caseSensitive: Boolean = false
  ): DataFrame = {
    val dfColMap =
      df.columns.map(c => normalizeColumnName(c, caseSensitive) -> c).toMap
    val conformedColumns = schema.fields.map { field =>
      val normalizedName = normalizeColumnName(field.name, caseSensitive)
      dfColMap
        .get(normalizedName)
        .map(origCol => castColumn(origCol, field.dataType))
        .getOrElse(lit(null).cast(field.dataType))
        .as(field.name)
    }
    df.select(conformedColumns: _*)
  }

  def filterToSchema(
      df: DataFrame,
      schema: StructType,
      caseSensitive: Boolean = false
  ): DataFrame = {
    val schemaColumns =
      schema.fields.map(f => normalizeColumnName(f.name, caseSensitive)).toSet
    val filteredColumns = df.columns
      .filter(c =>
        schemaColumns.contains(normalizeColumnName(c, caseSensitive))
      )
      .map(col)
    castToSchema(df.select(filteredColumns: _*), schema, caseSensitive)
  }

  /** Convert nested Spark types to JSON strings for proper round-trip handling
    * (round-trip) = Df -> TestD -> Df ... without any loss for a given schema
    */
  def fromDf(df: DataFrame): TestD[Map[String, Any]] = {
    /* Recursively convert Spark values to JSON format */
    def sparkValueToJson(value: Any): String = value match {
      case null => "null"
      case row: GenericRowWithSchema =>
        val fields = row.schema.fields.zipWithIndex.map { case (field, idx) =>
          val fieldValue = if (row.isNullAt(idx)) null else row.get(idx)
          s""""${field.name}":${sparkValueToJson(fieldValue)}"""
        }
        s"{${fields.mkString(",")}}"
      case seq: mutable.WrappedArray[_] =>
        s"[${seq.map(sparkValueToJson).mkString(",")}]"
      case map: scala.collection.Map[_, _] =>
        val pairs = map
          .map { case (k, v) => s""""$k":${sparkValueToJson(v)}""" }
          .mkString(",")
        s"{$pairs}"
      case str: String => s""""$str""""
      case other       => other.toString
    }

    /** Convert complex types to JSON, simple types to strings */
    def convertValue(value: Any, dataType: DataType): Any = {
      if (value == null) return null
      dataType match {
        case _: StructType | _: ArrayType | _: MapType =>
          /* Strip outer quotes from string values to avoid double-quoting */
          sparkValueToJson(value) match {
            case json if json.startsWith("\"") && json.endsWith("\"") =>
              json.substring(1, json.length - 1)
            case json => json
          }
        case _ => value.toString
      }
    }

    val rows = df
      .collect()
      .map { row =>
        df.schema.fields.zipWithIndex.map { case (field, idx) =>
          val value = if (row.isNullAt(idx)) null else row.get(idx)
          field.name -> convertValue(value, field.dataType)
        }.toMap
      }
      .toSeq

    new TestD(rows)
  }
}
