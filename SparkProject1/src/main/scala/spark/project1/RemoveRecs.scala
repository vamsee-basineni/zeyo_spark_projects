package spark.project1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object RemoveRecs {
  
    def main(args: Array[String]): Unit = {
      
        val spark = SparkSession.builder().master("local[1]").appName("RemoveRecs").getOrCreate()
        import spark.implicits._
        val sc=spark.sparkContext
        sc.setLogLevel("ERROR")
        println
        println("################# SOLUTION 1 - using lists ####################")
        println("=================file1====================")
        val file1df = spark.read.format("csv")
                                .option("delimiter",",")
                                .option("header","true")
                                .load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/file1.txt")
        file1df.printSchema()
        file1df.show()
        println("=================file2====================")
        val file2df = spark.read.format("csv")
                                .option("delimiter",",")
                                .option("header","true")
                                .load("file:///C:/Users/vamse/Downloads/batch28_zeyo_data/file2.txt")
        file2df.printSchema()
        file2df.show()
        
        println("=================list of ids from file1================")
        val lst1 = file1df.select("id").collect().map(_(0)).toList      //converting df(ids) to list(ids)
        println(lst1)
        
        println
        println("=================list of ids from file2================")
        val lst2 = file2df.select("id").collect().map(_(0)).toList      //converting df(ids) to list(ids)
        println(lst2)
        
        println
        println("=================List of ids in file1 and not in file2==============")
        val diffLst = lst1 diff lst2
        println(diffLst)
        println
        println("=================List of ids in file2 and not in file1==============")
        val diffLst2 = lst2 diff lst1
        println(diffLst2)
        println
        println("================filter ids in file1 and not in file2================")
        val idsin1notin2 = file1df.filter(col("id").isin(diffLst:_*))
        idsin1notin2.show(false)
        println
        println("================filter ids in file2 and not in file1================")
        val idsin2notin1 = file2df.filter(col("id").isin(diffLst2:_*))
        idsin2notin1.show()
        
        //Supported join types: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 
        //'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'leftanti', 'left_anti', 'cross'.
        println("################# SOLUTION 2 - using left-anti join ####################")
        println
        println("================filter ids in file1 and not in file2================")
        val resultdf1 = file1df.join(file2df, file1df.col("id") === file2df.col("id"), "left_anti")
        resultdf1.show()
        
        println
        println("================filter ids in file2 and not in file1================")
        val resultdf2 = file2df.join(file1df, file2df.col("id") === file1df.col("id"), "leftanti")
        resultdf2.show()
        
    }
  
}