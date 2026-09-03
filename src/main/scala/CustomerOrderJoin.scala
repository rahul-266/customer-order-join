import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CustomerOrderJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Customer Order Join")
      .master("local[*]")
      .getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import spark.implicits._

    // CUSTOMER DATA
    val customers = Seq(
      (1, "Chintan", "Hyderabad"),
      (2, "Rahul", "Mumbai"),
      (3, "Priya", "Bangalore"),
      (4, "Amit", "Delhi")
    ).toDF(
      "customer_id",
      "customer_name",
      "city"
    )

    // ORDER DATA
    val orders = Seq(
      (101, 1, 50000),
      (102, 2, 20000),
      (103, 1, 25000),
      (104, 3, 30000),
      (105, 4, 15000),
      (106, 2, 40000)
    ).toDF(
      "order_id",
      "customer_id",
      "amount"
    )

    // JOIN
    val joinedData = customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "inner"
      )
      .select(
        customers("customer_id"),
        customers("customer_name"),
        customers("city"),
        orders("order_id"),
        orders("amount")
      )

    println("===== JOINED DATA =====")
    joinedData.show()

    // TOTAL SPENDING FOR EACH CUSTOMER
    val customerTotals = joinedData
      .groupBy(
        "customer_id",
        "customer_name",
        "city"
      )
      .agg(
        sum("amount").alias("total_spending")
      )

    println("===== CUSTOMER TOTALS =====")
    customerTotals.show()

    // WINDOW RANKING
    val windowSpec =
      Window
        .partitionBy("customer_id")
        .orderBy(desc("amount"))

    val finalResult = joinedData
      .join(
        customerTotals,
        Seq("customer_id", "customer_name", "city"),
        "inner"
      )
      .withColumn(
        "order_rank",
        row_number().over(windowSpec)
      )
      .orderBy(
        "customer_id",
        "order_rank"
      )

    println("===== FINAL RESULT =====")
    finalResult.show()

    println("===== EXECUTION PLAN =====")
    joinedData.explain(true)

    spark.stop()
  }
}
