package alert.system
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object createStreamSource {
  
   def main (args: Array[String]) {
       
       val tempDir = System.getProperty("user.dir")
       val path = tempDir + "/warehouse"
       val spark = SparkSession.builder()
                               .appName("BuildStreamSource")
                               .master("local[*]")
                               .config("spark.sql.warehouse.dir", path)
                               .getOrCreate()
      
       spark.sparkContext.setLogLevel("ERROR")
       spark.sparkContext.setCheckpointDir("/checkpoints")
       
       val run_path= System.getProperty("user.dir")
       val sensor_data_path= run_path.concat("\\sensor_data.csv\\")
       val source_path= run_path.concat("\\streamsource")
       
       val sensor_full_data = spark.read
                                   .format("csv")
                                   .option("header", "true")
                                   .option("inferSchema","true")
                                   .load(sensor_data_path)
       sensor_full_data.createOrReplaceTempView("sensor_data_raw")
       
       for (i <- 1 to 7) {
         
         val partition_data = spark.sql(
              "select "+
              "Date, "+
              "Time, "+
              "Tambient, "+
              "Tcontainer_1, "+
              "Tcontainer_2, "+
              "Pyranometer_Gt_Row1, "+
              "Pyranometer_Gt_Row2, "+
              "Pyranometer_Gt_Row3, "+
              "Pyranometer_Gt_Row4, "+
              "Pyranometer_Gh, "+
              "Humidity, "+
              "APE, "+
              "WD, "+
              "WS "+
             s"from sensor_data_raw where Iterator = $i"
         )
         
         partition_data.write
                       .mode("append")
                       .csv(source_path)
         Thread.sleep(30000)  //This thread will push data to streamsource folder every 30 seconds
       }
              
   }
}