import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._

case class TestData[T](data: Seq[T]) {
  require(data.nonEmpty, "TestData must contain at least one row (headers)")
  
  private def toSeq(row: Any): Seq[Any] = row match {
    case seq: Seq[_] => seq
    case tuple: Product => tuple.productIterator.toSeq
    case other => throw new IllegalArgumentException(s"Unsupported row type: ${other.getClass}")
  }
  
  val headers: Seq[Any] = toSeq(data.head).map {
    case s: String => s.toUpperCase
    case other => other
  }
  
  val rows: Seq[Seq[Any]] = data.tail.map(toSeq)
  
  private def formatRow(row: Seq[Any], columnWidths: Seq[Int], isHeader: Boolean = false): String = {
    row.zip(columnWidths).map { case (value, width) =>
      value match {
        case s: String => 
          val strValue = if (isHeader) s.toUpperCase else s
          s""""$strValue"""".padTo(width, ' ')
        case other => other.toString.padTo(width, ' ')
      }
    }.mkString(", ")
  }

  def toDf(spark: SparkSession): DataFrame = {
    val schema = StructType(headers.zip(rows.head).map { case (header, value) =>
      val dataType = value match {
        case _: String => StringType
        case _: Int => IntegerType
        case _: Long => LongType
        case _: Double => DoubleType
        case _: Boolean => BooleanType
        case _ => StringType
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
      allRows.map(row => 
        row(i) match {
          case s: String => s""""$s"""".length
          case other => other.toString.length
        }
      ).max
    }
    
    val headerFormatted = formatRow(headers, columnWidths, true)
    val formattedRows = rows.map(row => formatRow(row, columnWidths))
    
    val prefix = data.head match {
      case _: Seq[_] => "Seq"
      case _: Product => ""
      case _ => throw new IllegalArgumentException("Unsupported header type")
    }
    
    val rowPrefix = if (prefix.isEmpty) "  (" else s"  $prefix("
    
    s"""TestData(Seq(
       |$rowPrefix$headerFormatted),
       |${formattedRows.map(row => s"$rowPrefix$row)").mkString(",\n")}
       |))""".stripMargin
  }
}

object TestData {
  def apply[T](data: Seq[T]): TestData[T] = new TestData(data)
}

