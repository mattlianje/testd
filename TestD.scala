package testd

case class TestD[T](data: Seq[T]) {
  import org.apache.spark.sql.{SparkSession, DataFrame, Row}
  import org.apache.spark.sql.types._

  require(data.nonEmpty, "TestD must contain at least one row (headers)")

  private def toSeq(row: Any): Seq[Any] = row match {
    case seq: Seq[_]    => seq
    case tuple: Product => tuple.productIterator.toSeq
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported row type: ${other.getClass}"
      )
  }

  val headers: Seq[Any] = toSeq(data.head).map {
    case s: String => s.toUpperCase
    case other     => other
  }

  val rows: Seq[Seq[Any]] = data.tail.map(toSeq)

  def toDf(spark: SparkSession): DataFrame = {
    val schema = StructType(headers.zip(rows.head).map { case (header, value) =>
      val dataType = value match {
        case _: String  => StringType
        case _: Int     => IntegerType
        case _: Long    => LongType
        case _: Double  => DoubleType
        case _: Boolean => BooleanType
        case _          => StringType
      }
      StructField(header.toString, dataType, nullable = true)
    })

    val sparkRows = rows.map(row => Row.fromSeq(row))
    spark.createDataFrame(
      spark.sparkContext.parallelize(sparkRows),
      schema
    )
  }

  override def toString: String = {
    val allRows = data.map(toSeq)
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
            case other => other.toString.padTo(width, ' ')
          }
        }
        .mkString(", ")
    }

    val headerFormatted = formatRow(headers, true)
    val formattedRows = rows.map(row => formatRow(row))
    val prefix = data.head match {
      case _: Seq[_]  => "Seq"
      case _: Product => ""
      case _ => throw new IllegalArgumentException("Unsupported header type")
    }
    val rowPrefix = if (prefix.isEmpty) "  (" else s"  $prefix("

    s"""TestD(Seq(
       |$rowPrefix$headerFormatted),
       |${formattedRows.map(row => s"$rowPrefix$row)").mkString(",\n")}
       |))""".stripMargin
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
