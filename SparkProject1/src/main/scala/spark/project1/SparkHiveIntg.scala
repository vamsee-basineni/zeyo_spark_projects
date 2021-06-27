package spark.project1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

object SparkHiveIntg {
  
        def main(args: Array[String]): Unit = {
          
              val spark=SparkSession.builder().master("local[1]")
                                              .appName("NullHandling")
                                              .enableHiveSupport()
                                              .getOrCreate()
              import spark.implicits._
              
              val sc=spark.sparkContext
              sc.setLogLevel("ERROR")
              
              //val hc=new HiveContext(sc)
              //import hc.implicits._
              
              println
              println("===========Hive table count===========")
              spark.sql("select count(*) from txns.spark_to_hive_txns").show()
          
        }
}