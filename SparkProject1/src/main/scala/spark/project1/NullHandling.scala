package spark.project1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object NullHandling {
      
        def main(args: Array[String]): Unit = {
          
              val spark=SparkSession.builder().master("local[1]").appName("NullHandling").getOrCreate()
              import spark.implicits._
              val sc=spark.sparkContext
              sc.setLogLevel("ERROR")
              
              val hc=new HiveContext(sc)
              import hc.implicits._
              
              println("============Raw nulldf printSchema and print data=============")
              val nulldf = spark.read.format("csv").option("header","true").option("inferSchema","true")
                                     .load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/nulldata.txt")
              
              nulldf.printSchema()
              nulldf.show()
              
             /* root
             |-- id: integer (nullable = true)
             |-- name: string (nullable = true)
             |-- amount: integer (nullable = true)
             |-- place: string (nullable = true)

             +----+------+------+-----+
             |  id|  name|amount|place|
             +----+------+------+-----+
             |   1|vamsee|    40|  CHN|
             |   2|  null|    70| BANG|
             |null|  zeyo|  null| null|
             +----+------+------+-----+*/
              
              println("=========Replace nulls in all integer columns (id, amount) with 0============")
              
              val repNullIntsDf = nulldf.na.fill(0) 
              repNullIntsDf.printSchema()
              repNullIntsDf.show()
              
              /*nulldf.na.fill(0) --- Replace nulls in all integer columns with 0
               * root
 								 |-- id: integer (nullable = false)
 								 |-- name: string (nullable = true)
 								 |-- amount: integer (nullable = false)
 								 |-- place: string (nullable = true)
 								 
              +---+------+------+-----+
              | id|  name|amount|place|
              +---+------+------+-----+
              |  1|vamsee|    40|  CHN|
              |  2|  null|    70| BANG|
              |  0|  zeyo|     0| null|
              +---+------+------+-----+*/
              
              println("==========Replace Nulls with 0s in specific integer column id============")
              
              val repNullsIntColDf = nulldf.na.fill(0,Array("id"))
              repNullsIntColDf.printSchema()
              repNullsIntColDf.show()
              
              /*root
 								|-- id: integer (nullable = false)
 								|-- name: string (nullable = true)
 								|-- amount: integer (nullable = true)
 								|-- place: string (nullable = true)
 
              +---+------+------+-----+
              | id|  name|amount|place|
              +---+------+------+-----+
              |  1|vamsee|    40|  CHN|
              |  2|  null|    70| BANG|
              |  0|  zeyo|  null| null|
              +---+------+------+-----+*/
              
              println("=========Replace nulls in all string columns (name, place) with NA============")
              
              val repNullStrDf = nulldf.na.fill("NA") //All String columns
              repNullStrDf.printSchema()
              repNullStrDf.show()
              
              /*root
              |-- id: integer (nullable = true)
              |-- name: string (nullable = false)
              |-- amount: integer (nullable = true)
              |-- place: string (nullable = false)

              +----+------+------+-----+
              |  id|  name|amount|place|
              +----+------+------+-----+
              |   1|vamsee|    40|  CHN|
              |   2|    NA|    70| BANG|
              |null|  zeyo|  null|   NA|
              +----+------+------+-----+*/
              
              println("==========Replace Nulls with NA in specific string column place============")
              
              val repNullsStrColDf = nulldf.na.fill("NA",Array("place"))
              repNullsStrColDf.printSchema()
              repNullsStrColDf.show()
              
              /*root
               |-- id: integer (nullable = true)
               |-- name: string (nullable = true)
               |-- amount: integer (nullable = true)
               |-- place: string (nullable = false)

              +----+------+------+-----+
              |  id|  name|amount|place|
              +----+------+------+-----+
              |   1|vamsee|    40|  CHN|
              |   2|  null|    70| BANG|
              |null|  zeyo|  null|   NA|
              +----+------+------+-----+*/
              
              println("==========Replace Null string cols with NA and Null int cols with 0 in all integer and string columns============")
              
              val repNullsAllColDf = nulldf.na.fill("NA").na.fill(0)
              repNullsAllColDf.printSchema()
              repNullsAllColDf.show()
              
              /*root
               |-- id: integer (nullable = false)
               |-- name: string (nullable = false)
               |-- amount: integer (nullable = false)
               |-- place: string (nullable = false)

              +---+------+------+-----+
              | id|  name|amount|place|
              +---+------+------+-----+
              |  1|vamsee|    40|  CHN|
              |  2|    NA|    70| BANG|
              |  0|  zeyo|     0|   NA|
              +---+------+------+-----+*/
              
              println("============Add current date and current timestamp columns to nulldf===========")
              
              val addCurrDateTsDf = nulldf.withColumn("curr_date",current_date())
                                          .withColumn("curr_timestamp",current_timestamp())
              
              addCurrDateTsDf.printSchema()
              addCurrDateTsDf.show(false)
              
        }
}