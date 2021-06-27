package spark.project1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object DfToStringConversion {
  
        def main(args: Array[String]): Unit = {
          
              val spark=SparkSession.builder().master("local[1]").appName("NullHandling").getOrCreate()
              import spark.implicits._
              val sc=spark.sparkContext
              sc.setLogLevel("ERROR")
              val hc=new HiveContext(sc)
              import hc.implicits._
              
              val txns_schema = StructType(List(
            		  StructField("txnno",IntegerType,true),
            		  StructField("txndate",StringType,true),
            		  StructField("custno",IntegerType,true),
            		  StructField("amount",DoubleType,true),
            		  StructField("category",StringType,true),
            		  StructField("product",StringType,true),
            		  StructField("city",StringType,true),
            		  StructField("state",StringType,true),
            		  StructField("spendby",StringType,true)
            		  ))
            	println
            	println("===========txns raw df printschema and print data=============")
            	val txnsdf = spark.read.format("csv").schema(txns_schema).load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/txns")
            	txnsdf.printSchema()
            	txnsdf.show(5, false)
            	
            	println
            	println("=============converting dataframe to string==============")
              println("Solution #1")
            	val max_txn_no = txnsdf.agg(max(col("txnno"))).collect.map(x=>x.getInt(0)).mkString
            	println("max_txn_no: " + max_txn_no)
              
              println("Solution #2")
              val maxval = txnsdf.selectExpr("max(txnno)")  // depends on the column name if u read with header it would be txnno
              val maxvalue=maxval.collect().map(x=>x.mkString("")).mkString("").toInt
              println("maxvalue: " + maxvalue)
        }
}