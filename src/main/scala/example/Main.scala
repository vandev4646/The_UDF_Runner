package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    //Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("TPCH-UDF-Runner")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Define TPC-H Schemas
    val customerSchema = StructType(Seq(
      StructField("c_custkey", LongType),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", IntegerType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DoubleType),
      StructField("c_mktsegment", StringType),
      StructField("c_comment", StringType)
    ))

    val ordersSchema = StructType(Seq(
      StructField("o_orderkey", LongType),
      StructField("o_custkey", LongType),
      StructField("o_orderstatus", StringType),
      StructField("o_totalprice", DoubleType),
      StructField("o_orderdate", StringType), // Loaded as String for simplicity with CSV reader
      StructField("o_orderpriority", StringType),
      StructField("o_clerk", StringType),
      StructField("o_shippriority", IntegerType),
      StructField("o_comment", StringType)
    ))

    //Load TPC-H Data into spark
    val tpchPath = "tpchData"

    val customerDF = spark.read
      .option("delimiter", "|")
      .schema(customerSchema)
      .csv(s"$tpchPath/customer.tbl")

    val ordersDF = spark.read
      .option("delimiter", "|")
      .schema(ordersSchema)
      .csv(s"$tpchPath/orders.tbl")

    customerDF.createOrReplaceTempView("customer")
    ordersDF.createOrReplaceTempView("orders")

    //Mock Data for Custom Tables (Not in TPC-H)
    val prefsDF = Seq(
      (1L, "EUR"), (2L, "GBP"), (3L, "JPY"), (4L, "CAD")
    ).toDF("custkey", "currency")
    prefsDF.createOrReplaceTempView("customer_prefs")

    val xchgDF = Seq(
      ("USD", "EUR", 0.92),
      ("USD", "GBP", 0.78),
      ("USD", "JPY", 150.0),
      ("USD", "CAD", 1.35)
    ).toDF("from_cur", "to_cur", "rate")
    xchgDF.createOrReplaceTempView("xchg")

    // Register the UDF
    spark.udf.register("total_price", (totalPrice: Double, rate: Double, prefCurrency: String) => {
      val defaultCurrency = "USD"
      val finalPrice = if (prefCurrency != defaultCurrency) totalPrice * rate else totalPrice
      f"$finalPrice%.2f $prefCurrency"
    })

    
    val result = spark.sql("""
      WITH customer_totals AS (
        SELECT o_custkey, SUM(o_totalprice) as total_sum
        FROM orders
        GROUP BY o_custkey
      )
      SELECT 
        c.c_name, 
        total_price(
          COALESCE(t.total_sum, 0.0), 
          COALESCE(x.rate, 1.0), 
          COALESCE(p.currency, 'USD')
        ) as formatted_total
      FROM customer c
      LEFT JOIN customer_totals t ON c.c_custkey = t.o_custkey
      LEFT JOIN customer_prefs p ON c.c_custkey = p.custkey
      LEFT JOIN xchg x ON p.currency = x.to_cur AND x.from_cur = 'USD'
      LIMIT 20
    """)

    // 7. Show Results
    result.show(truncate = false)

    spark.stop()
  }
}
