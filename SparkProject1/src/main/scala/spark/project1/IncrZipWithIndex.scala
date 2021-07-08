package spark.project1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object IncrZipWithIndex {
  
      def main(args: Array[String]): Unit = {
        
          val conf=new SparkConf().setMaster("local[1]").setAppName("IncrZipWithIndex")
          val sc=new SparkContext(conf)
          sc.setLogLevel("ERROR")
          
          val spark = SparkSession.builder().getOrCreate()
          import spark.implicits._
          
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
          println("===========txnsdf print schema and print few rows============")
          val txnsdf = spark.read.format("csv").schema(txns_schema).load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/txns")
          txnsdf.printSchema()
          txnsdf.show(10,false)
          
          println("===========Adding Incremental Index to txnsdf================")
          val incrIndexTxnsdf = addColumnIndex(spark, txnsdf)
          
          incrIndexTxnsdf.printSchema()
          incrIndexTxnsdf.show(false)
  
      }
      
      def addColumnIndex(spark: SparkSession, df: DataFrame) = {          //DataFrame is part of org.apache.spark.sql._ package
        
            spark.createDataFrame(                                        //spark.sqlContext.createDataFrame --- both will work
                
                df.rdd.zipWithIndex.map {
                  case (row, index) => Row.fromSeq(row.toSeq :+ index)
                },
                //create schema for index column
                StructType(df.schema.fields :+ StructField("index", LongType, false))
            )
      }
  
}