package spark.project1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import scala.io.Source._

object WebCustAnalytics {
  
    def main(args: Array[String]):Unit = {
      
          val spark = SparkSession.builder().master("local[1]").appName("WebAnalytics").getOrCreate()
          import spark.implicits._
          val sc=spark.sparkContext
          sc.setLogLevel("ERROR")
          
          println
          println("=============Step 2: Read Avro file as a dataframe==================")
          val avrodf = spark.read.format("com.databricks.spark.avro")
                                 .load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/spark_project1_data/part-00000.avro")                     
          avrodf.printSchema()
          avrodf.show()
          
          println("==============Step 3: Read Random API URL data================")
          val url="https://randomuser.me/api/0.8/?results=200"
          val urldata = sc.parallelize(List(scala.io.Source.fromURL(url).mkString))
          val urldf=spark.read.json(urldata)
          urldf.printSchema()
          urldf.show()
          
          println("============Step 4: Flatten URL Json data completely====================")
          val urlflatdf = urldf.withColumn("results",explode(col("results")))
                               .select(
                                    col("nationality"),
                                    col("results.user.cell").alias("user_cell"),
                                    col("results.user.dob").alias("user_dob"),
                                    col("results.user.email").alias("user_email"),
                                    col("results.user.gender").alias("user_gender"),
                                    col("results.user.location.city").alias("user_city"),
                                    col("results.user.location.state").alias("user_state"),
                                    col("results.user.location.street").alias("user_street"),
                                    col("results.user.location.zip").alias("user_zip"),
                                    col("results.user.md5").alias("user_md5"),
                                    col("results.user.name.first").alias("user_first_name"),
                                    col("results.user.name.last").alias("user_last_name"),
                                    col("results.user.name.title").alias("user_title_name"),
                                    col("results.user.password").alias("user_password"),
                                    col("results.user.phone").alias("user_phone"),
                                    col("results.user.picture.large").alias("user_large_pic"),
                                    col("results.user.picture.medium").alias("user_medium_pic"),
                                    col("results.user.picture.thumbnail").alias("user_thumbnail_pic"),
                                    col("results.user.registered").alias("user_registered"),
                                    col("results.user.salt").alias("user_salt"),
                                    col("results.user.sha1").alias("user_sha1"),
                                    col("results.user.sha256").alias("user_sha256"),
                                    col("results.user.username").alias("user_username"),
                                    col("seed"),
                                    col("version")
                                  )
          
          urlflatdf.printSchema()
          urlflatdf.show(false)
          
          println("===============Step 5: Remove Numericals from username of urlflatdf===================")
          val urlfinaldf = urlflatdf.withColumn("user_username", regexp_extract(col("user_username"), "[a-z]+", 0))
          urlfinaldf.show()
          
          println("===============Step 6: Broadcast Left Join Avro df with urlflatdf=====================")
          
          val bcleftjoindf = avrodf.join(broadcast(urlfinaldf),avrodf("username")===urlfinaldf("user_username"),"left")
                                   //.drop(col("user_username"))
          
          bcleftjoindf.printSchema()
          bcleftjoindf.show()
          
          println("================Step 7: Two dfs with Nationality Null and Not Null====================")
          val nonAvailCustTemp = bcleftjoindf.filter(col("nationality").isNull)
          
          println("-----------Step 7A: Non Available Customers df print----------------")
          nonAvailCustTemp.show()
          println
          
          println("-----------Step 7B: Available Customers df print----------------")
          val availCustTemp = bcleftjoindf.filter(col("nationality").isNotNull)
          availCustTemp.show()
          
          println("===============Step 8: Replace Nulls with NA and 0 and add current date in Non available customers df=================")
          val nonAvailCust = nonAvailCustTemp.na.fill("NA").na.fill(0)
                                             .withColumn("curr_date",current_date())
          nonAvailCust.show()
          
          println("===============Step 9: Add current date to available customer df===============")
          val availCust = availCustTemp.withColumn("curr_date",current_date())
          availCust.show()
          
          println("===============Step 10: Write both the dfs as json files=================")
          val finalAvailCustdf = availCust.groupBy(col("username")).agg(collect_list(col("id")).alias("id_array"),
                                                                       collect_list(col("ip")).alias("ip_array"),
                                                                       sum("amount").cast(DataTypes.createDecimalType(18,2)).alias("total_amount"),
                                                                       struct(
                                                                           count("ip").alias("ip_count"),
                                                                           count("id").alias("id_count")
                                                                         ).alias("id_ip_counts")
                                                                       )
                                                                       
          println("-------------Available customers group by username id, ip counts-----------------")
          finalAvailCustdf.printSchema()
          finalAvailCustdf.show()
          
          finalAvailCustdf.coalesce(1).write.format("json")
                                           .mode("overwrite")
                                           .save("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/spark_project1_data/avail_cust_json")
                                           
          println("=============available customers json written to local successfully==================")
                                           
          val finalNonAvailCustdf = nonAvailCust.groupBy("username").agg(collect_list("ip").alias("ip_array"),
                                                                         collect_list("id").alias("id_array"),
                                                                         sum("amount").cast(DataTypes.createDecimalType(18,2)).alias("total_amount"),
                                                                         struct(
                                                                               count("ip").alias("ip_count"),
                                                                               count("id").alias("id_count")
                                                                           ).alias("id_ip_counts")
                                                                        )
          
          println("-------------Non Available customers group by username id, ip counts-----------------")
          finalNonAvailCustdf.printSchema()
          finalNonAvailCustdf.show()
          
          finalNonAvailCustdf.coalesce(1).write.format("json")
                                               .mode("overwrite")
                                               .save("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/spark_project1_data/nonavail_cust_json")
                                               
          println("=============Non available customers json written to local successfully==================")
          
    }
  
}