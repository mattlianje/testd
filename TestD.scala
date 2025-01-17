package testd

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

  private def getAllHeaders: Seq[String] = {
    data
      .flatMap {
        case map: Map[_, _] => map.keys
        case _              => Seq.empty
      }
      .distinct
      .map(_.toString.toUpperCase)
      .sorted
  }

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

  def toMap: String = {
    val rowsAsMap = if (data.head.isInstanceOf[Map[_, _]]) {
      data.map(_.asInstanceOf[Map[String, Any]])
    } else {
      val columnNames = headers.map(_.toString)
      rows.map(row => columnNames.zip(row).toMap)
    }

    s"""TestD(Seq(
     |${rowsAsMap
        .map(m =>
          s"""  Map(${m.toSeq
              .sortBy(_._1)
              .map { case (k, v) =>
                v match {
                  case s: String => s""""$k" -> "$s""""
                  case null      => s""""$k" -> null"""
                  case other     => s""""$k" -> $other"""
                }
              }
              .mkString(", ")})"""
        )
        .mkString(",\n")}
     |))""".stripMargin
  }

  def toDf(spark: SparkSession): DataFrame = {
    val schema = headers.zipWithIndex.map { case (header, idx) =>
      val columnValues = rows.map(_(idx))
      val dataType = columnValues.headOption match {
        case Some(value) if !columnValues.contains(null) =>
          value match {
            case _: String  => StringType
            case _: Int     => StringType 
            case _: Long    => StringType 
            case _: Double  => StringType 
            case _: Boolean => StringType 
            case _          => StringType
          }
        case _ => StringType
      }
      StructField(header.toString, dataType, nullable = true)
    }

    val convertedRows = rows.map { row =>
      Row.fromSeq(row.map {
        case null => null
        case v    => v.toString
      })
    }

    spark.createDataFrame(
      spark.sparkContext.parallelize(convertedRows),
      StructType(schema)
    )
  }

  override def toString: String = {
    val allRows = data.head match {
      case _: Map[_, _] => Seq(headers) ++ rows
      case _            => data.map(toSeq)
    }

    val columnWidths = headers.indices.map { i =>
      allRows
        .map(row =>
          row(i) match {
            case s: String =>
              val str = if (row == headers) s.toUpperCase else s
              if (str.trim.matches("""^\[.*\]$|^\{.*\}$""")) {
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

    def formatRow(row: Seq[Any], isHeader: Boolean = false): String = {
      row
        .zip(columnWidths)
        .map { case (value, width) =>
          value match {
            case s: String =>
              val str = if (isHeader) s.toUpperCase else s
              if (str.trim.matches("""^\[.*\]$|^\{.*\}$""")) {
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

    val headerFormatted = formatRow(headers, true)
    val formattedRows = rows.map(row => formatRow(row))
    val prefix = data.head match {
      case _: Map[_, _]                      => ""
      case _: Seq[_]                         => "Seq"
      case _: Product if headers.length > 22 => "Seq"
      case _: Product                        => ""
      case _ => throw new IllegalArgumentException("Unsupported header type")
    }

    val useSeqPrefix = headers.length > 22
    val rowPrefix = if (useSeqPrefix) "  Seq(" else "  ("

    s"""TestD(Seq(
     |$rowPrefix$headerFormatted),
     |${formattedRows.map(row => s"$rowPrefix$row)").mkString(",\n")}
     |))""".stripMargin
  }

  def withColumn(name: String): TestD[Map[String, Any]] =
    withColumn(name, null.asInstanceOf[String])

  def withColumn(name: String, value: Any): TestD[Map[String, Any]] = {
    val newHeaders = headers :+ name.toUpperCase
    val newRows = rows.map(_ :+ value)

    new TestD[Map[String, Any]](
      if (data.head.isInstanceOf[Map[_, _]]) {
        data.map { row =>
          row.asInstanceOf[Map[String, Any]] + (name -> value)
        }
      } else {
        rows.map(row =>
          headers.map(_.toString).zip(row).toMap + (name -> value)
        )
      }
    )
  }

  def drop(colNames: String*): TestD[Map[String, Any]] = {
    val upperColNames = colNames.map(_.toUpperCase)

    new TestD[Map[String, Any]](
      if (data.head.isInstanceOf[Map[_, _]]) {
        data.map { row =>
          row.asInstanceOf[Map[String, Any]].filterNot { case (k, _) =>
            upperColNames.contains(k.toUpperCase)
          }
        }
      } else {
        rows.map(row =>
          headers
            .map(_.toString)
            .zip(row)
            .filterNot { case (k, _) => upperColNames.contains(k.toUpperCase) }
            .toMap
        )
      }
    )
  }
}

object TestD {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row, Column}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

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
      case _ =>
        col(colName).cast(dataType)
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
    val dfColMap = df.columns
      .map(c => normalizeColumnName(c, caseSensitive) -> c)
      .toMap

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
    val schemaColumns = schema.fields
      .map(f => normalizeColumnName(f.name, caseSensitive))
      .toSet

    val filteredColumns = df.columns
      .filter(c =>
        schemaColumns.contains(normalizeColumnName(c, caseSensitive))
      )
      .map(col)

    castToSchema(
      df.select(filteredColumns: _*),
      schema,
      caseSensitive
    )
  }
}
