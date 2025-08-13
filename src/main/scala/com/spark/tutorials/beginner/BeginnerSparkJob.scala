package com.spark.tutorials.beginner

import com.spark.tutorials.{BaseSparkSession, DataframeSet}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, coalesce, col, count, lit, sum, to_date, trim}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


class BeginnerSparkJob extends BaseSparkSession {
  import spark.implicits._
  case class InputDataFrames(customersDataFrame: DataFrame, agentsDataFrame: DataFrame, ordersDataFrame: DataFrame) extends DataframeSet


  override protected def loadData(inputPath: String): DataframeSet = {
    val customersDataFrame = spark.read.option("header", "false")
      .csv(s"""$inputPath/customer/customers.csv""")
    val agentsDataFrame = spark.read.option("header", "false")
      .csv(s"""$inputPath/customer/agents.csv""")
    val ordersDataFrame = spark.read.option("header", "false")
      .csv(s"""$inputPath/customer/orders.csv""")
    InputDataFrames(customersDataFrame, agentsDataFrame, ordersDataFrame)
  }


  override protected def process[T <: DataframeSet](dfSet: T): Unit = {

    dfSet match {
      case dfs: InputDataFrames =>
        //creating temp views to play around using sql

        val agentsDataFrameSch = dfs.agentsDataFrame.toDF("AGENT_CODE","AGENT_NAME", "WORKING_AREA", "COMMISSION", "PHONE_NO", "COUNTRY")

        val ordersDataFrameSch = dfs.ordersDataFrame.toDF("ORD_NUM", "ORD_AMOUNT", "ADVANCE_AMOUNT", "ORD_DATE",  "CUST_CODE", "AGENT_CODE","ORD_DESCRIPTION")

        val customersDataFrameSch = dfs.customersDataFrame.toDF("CUST_CODE", "CUST_NAME","CUST_CITY", "WORKING_AREA", "CUST_COUNTRY", "GRADE", "OPENING_AMT", "RECEIVE_AMT", "PAYMENT_AMT", "OUTSTANDING_AMT", "PHONE_NO", "AGENT_CODE")


        agentsDataFrameSch.createOrReplaceTempView("agentsDataFrame")
        ordersDataFrameSch.createOrReplaceTempView("ordersDataFrame")
        customersDataFrameSch.createOrReplaceTempView("customersDataFrame")

        // Select all agents:
//        val dfWithSchema = dfs.agentsDataFrame.toDF("AGENT_CODE",
//          "AGENT_NAME", "WORKING_AREA", "COMMISSION", "PHONE_NO", "COUNTRY")

        //displays 20 rows
//        dfWithSchema.select("AGENT_CODE", "AGENT_NAME",
//          "WORKING_AREA", "COMMISSION", "PHONE_NO", "COUNTRY").show()

        //shows 20 rows without truncating column name
//        dfWithSchema.select("AGENT_CODE", "AGENT_NAME",
//          "WORKING_AREA", "COMMISSION", "PHONE_NO", "COUNTRY").show(truncate = false)

        //shows all rows without truncating and displays data vertically
//        dfWithSchema.select("AGENT_CODE", "AGENT_NAME",
//          "WORKING_AREA", "COMMISSION", "PHONE_NO", "COUNTRY").show(10000,truncate = 1000, vertical = true)

        // select query using spark sql
//        spark.sql("select * from customersDataFrame").show()
        /*
        * Find all customers from a specific city (e.g., 'New York'):
          sql
          SELECT CUST_NAME, CUST_CITY
          FROM CUSTOMER
          WHERE CUST_CITY = 'New York';
        *
        * */
//        spark.sql("""select CUST_NAME, CUST_CITY from customersDataFrame where TRIM(CUST_CITY) = 'New York' """).show()

//        customersDataFrameSch.select("CUST_NAME", "CUST_CITY").filter(col("CUST_CITY") =!= lit("New York")).show()
//        customersDataFrameSch.select("CUST_NAME", "CUST_CITY").filter(col("CUST_CITY").like("%New York%")).show()
//        customersDataFrameSch.select("CUST_NAME", "CUST_CITY").filter(trim(col("CUST_CITY")) === lit("New York")).show()



        //trims spaces
//        val trimmedDF: DataFrame = dfs.customersDataFrame.schema.fields.foldLeft(dfs.customersDataFrame) { (tempDF, field) =>
//          field.dataType.simpleString match {
//            case "string" => tempDF.withColumn(field.name, trim(col(field.name)))
//            case _        => tempDF
//          }
//        }


      /***
       * List all orders with their order amount greater than 10,000:
       * sql
       * SELECT ORD_NUM, ORD_AMOUNT
       * FROM ORDERS
       * WHERE ORD_AMOUNT > 10000;
       */

//    spark.sql("""select ORD_NUM, ORD_AMOUNT from ordersDataFrame where CAST(ORD_AMOUNT AS DOUBLE) < 1000.00""").show()
//    spark.sql("""select ORD_NUM, coalesce(CAST(ORD_AMOUNT AS INT), 0) as ORD_AMT from ordersDataFrame where ORD_NUM = '200131'""").show()

//      ordersDataFrameSch.select("ORD_NUM", "ORD_AMOUNT")
//        .filter(col("ORD_AMOUNT").cast(DoubleType).lt(lit("1000.00"))).show()

//      ordersDataFrameSch.withColumn("ORD_AMOUNT", coalesce(col("ORD_AMOUNT").cast(DoubleType), lit(0.0)))
//        .select("ORD_NUM", "ORD_AMOUNT").filter(col("ORD_NUM") === "200131").show()

     /***
      * Find customers along with their agent names:
      * sql
      * SELECT C.CUST_NAME, A.AGENT_NAME
      * FROM CUSTOMER C
      * JOIN AGENTS A ON C.AGENT_CODE = A.AGENT_CODE;
      * */

//      spark.sql(
//        """select C.CUST_NAME, A.AGENT_NAME
//          |from customersDataFrame C inner join agentsDataFrame A
//          |on (C.AGENT_CODE = A.AGENT_CODE)""".stripMargin).show()

//        customersDataFrameSch.join(agentsDataFrameSch, Seq("AGENT_CODE"), "inner")
//          .select(customersDataFrameSch("CUST_NAME"), agentsDataFrameSch("AGENT_NAME"))
//          .show()

//        customersDataFrameSch.join(agentsDataFrameSch, customersDataFrameSch("AGENT_CODE") === agentsDataFrameSch("AGENT_CODE"), "inner")
//          .select(customersDataFrameSch("CUST_NAME"), agentsDataFrameSch("AGENT_NAME"))
//          .show()

//        customersDataFrameSch.join(agentsDataFrameSch, customersDataFrameSch("AGENT_CODE") === agentsDataFrameSch("AGENT_CODE"), "right_outer")
//          .select(customersDataFrameSch("CUST_NAME"), agentsDataFrameSch("AGENT_NAME"))
//          .show()

//        customersDataFrameSch.join(agentsDataFrameSch, customersDataFrameSch("AGENT_CODE") === agentsDataFrameSch("AGENT_CODE"), "cross")
//          .select(customersDataFrameSch("CUST_NAME"), agentsDataFrameSch("AGENT_NAME"))
//          .show()

        /***
         *
         * List orders placed between two dates:
         * sql
         * SELECT *
         * FROM ORDERS
         * WHERE ORD_DATE BETWEEN TO_DATE('2025-01-01', 'YYYY-MM-DD') AND TO_DATE('2025-06-30', 'YYYY-MM-DD');
         */
//        spark.sql("""select * from ordersDataFrame where TO_DATE(TRIM(ORD_DATE), 'MM/dd/yyyy') BETWEEN DATE('2008-01-01') AND DATE('2008-06-01') """).show()

//        ordersDataFrameSch.where(
//          to_date(trim(col("ORD_DATE")), "MM/dd/yyyy")
//            .between(lit("2008-01-01"), lit("2008-06-01"))
//        ).show()

      /***
       * Find the total outstanding amount of all customers:
       * sql
       * SELECT SUM(OUTSTANDING_AMT) AS TOTAL_OUTSTANDING
       * FROM CUSTOMER;
       */

//      spark.sql("""select sum(OUTSTANDING_AMT) from customersDataFrame""").show()

//      customersDataFrameSch.agg(sum("OUTSTANDING_AMT").alias("total_outstanding")).show()

      /***
       * Retrieve all agents who work in 'New York' area and have a commission above 0.15:
       * sql
       * SELECT *
       * FROM AGENTS
       * WHERE WORKING_AREA = 'New York' AND COMMISSION > 0.15;
       */

//      spark.sql("""select * from agentsDataFrame where trim(WORKING_AREA) = 'Bangalore' AND COMMISSION > 0.16 """).show()

      /***
       * List all customers sorted by their grade in descending order:
       * sql
       * SELECT CUST_NAME, GRADE
       * FROM CUSTOMER
       * ORDER BY GRADE DESC;
        */
//      spark.sql("select CUST_NAME, GRADE from customersDataFrame order by GRADE DESC").show()
//        customersDataFrameSch.select($"CUST_NAME", $"GRADE").orderBy($"GRADE".desc).show()

      /***
       *
       Advanced Level Queries

       Get the total order amount, total advance paid by each customer:
       sql
       SELECT C.CUST_NAME,
       SUM(O.ORD_AMOUNT) AS TOTAL_ORDER_AMOUNT,
       SUM(O.ADVANCE_AMOUNT) AS TOTAL_ADVANCE
       FROM CUSTOMER C
       LEFT JOIN ORDERS O ON C.CUST_CODE = O.CUST_CODE
       GROUP BY C.CUST_NAME;
       */

//      spark.sql(
//        """select C.CUST_NAME
//          |, SUM(cast(O.ORD_AMOUNT as DOUBLE)) as TOTAL_ORDER_AMOUNT
//          |, SUM(cast(O.ADVANCE_AMOUNT as DOUBLE)) AS TOTAL_ADVANCE
//          |FROM customersDataFrame C LEFT JOIN ordersDataFrame O ON trim(C.CUST_CODE)= trim(O.CUST_CODE)
//          |GROUP BY C.CUST_NAME""".stripMargin).show()

//        customersDataFrameSch
//          .join(ordersDataFrameSch,
//            trim(customersDataFrameSch("CUST_CODE")) === trim(ordersDataFrameSch("CUST_CODE")), "left_outer")
//          .groupBy(trim(customersDataFrameSch("CUST_CODE")).alias("TRIMMED_CUST_CODE"))
//          .agg(sum(ordersDataFrameSch("ORD_AMOUNT").cast(DoubleType)).alias("TOTAL_ORDER_AMOUNT"),
//            sum(ordersDataFrameSch("ADVANCE_AMOUNT").cast(DoubleType)).alias("TOTAL_ADVANCE")
//          ).select(col("TRIMMED_CUST_CODE"), col("TOTAL_ORDER_AMOUNT"), col("TOTAL_ORDER_AMOUNT")).show()

      /***
       * 6. Inline View with ROW_NUMBER()
       * Get the largest order for each customer (inline view for ranking):
       * sql
       * SELECT ORD_NUM, CUST_CODE, ORD_AMOUNT
       * FROM (
       * SELECT ORD_NUM, CUST_CODE, ORD_AMOUNT,
       * ROW_NUMBER() OVER (PARTITION BY CUST_CODE ORDER BY ORD_AMOUNT DESC) AS rn
       * FROM ORDERS
       * )
       * WHERE rn = 1;
       */

//      spark.sql(
//        """select rn, ORD_NUM, CUST_CODE, ORD_AMOUNT from (
//          |   select ORD_NUM
//          |   , CUST_CODE
//          |   , ORD_AMOUNT
//          |   , ROW_NUMBER() OVER (PARTITION BY trim(CUST_CODE) ORDER BY ORD_AMOUNT DESC) as rn
//          |   from ordersDataFrame
//          |
//          |) where rn > 0 order by rn DESC""".stripMargin).show(1000)


      /**
       * Query to show data as Map(String, List)
       */

//      spark.sql(
//        """select CUST_CODE, AGENT_CODE, count(1)
//          |from ordersDataFrame group by CUST_CODE, AGENT_CODE""".stripMargin).show()
//
//      spark.sql("""select CUST_CODE, AGENT_CODE, collect_list(ORD_AMOUNT) as lst
//                  |from ordersDataFrame group by CUST_CODE, AGENT_CODE""".stripMargin).show(100,100)


    val lst:List[Integer] = List(1,2,null,4,5,null,7,8)
        val df = lst.toDF("ID")

        val windowSpec = Window.orderBy(col("ID"))
        df.withColumn("ID", row_number().over(windowSpec) ).show(1000)



































































    }

  }

  override protected def save(outputPath: String): Unit = {

  }
}
