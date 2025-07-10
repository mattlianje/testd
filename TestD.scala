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

    s"TestD(Seq(\n$formattedMaps\n))"
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

    s"""TestD(Seq(
      |$rowPrefix$headerFormatted),
      |${formattedRows.map(row => s"$rowPrefix$row)").mkString(",\n")}
      |))""".stripMargin
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
}

object TestD {
  import org.apache.spark.sql.{DataFrame, Column}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
  import scala.collection.mutable

  def apply[T](data: Seq[T]): TestD[T] = new TestD(data)

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
