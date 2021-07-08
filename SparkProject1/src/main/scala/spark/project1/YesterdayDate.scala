package spark.project1

import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import java.util.Calendar

import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object YesterdayDate {
  
  
    def main(args: Array[String]): Unit = {
      
      println("==========Solution #1==========")
      //val dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
      val dateFmt = new SimpleDateFormat("yyyy-MM-dd")
      println("dateFmt: " + dateFmt)
      val tz = TimeZone.getTimeZone("UTC")
      dateFmt.setTimeZone(tz)
      val currTime = dateFmt.format(new Date())
      println("Current Date Time: " + currTime)
      println
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DATE, -1)
      val yestDate = dateFmt.format(calendar.getTime)
      println("Yesterdays Date: " + yestDate)
      
      println("==========Solution #2==========")
      println
      val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)
      //val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'")
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val yestDate2 = formatter format yesterday
      println("Yesterdays Date2: " + yestDate2)
      
      println("==========Solution #3==========")
      println
      val today = java.time.LocalDate.now
      println("today: " + today)
      
      val yestday = java.time.LocalDate.now.minusDays(1)
      println("yesterday: " + yestday)
      
      println
      val format = new SimpleDateFormat("MM-dd-yyyy")
      println("Current Date #2: " + format.format(Calendar.getInstance().getTime()))
      
    }
}