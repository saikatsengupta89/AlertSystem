package alert.system
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object readSensorData {
  
  def main (args: Array[String]) {
    
      val tempDir = System.getProperty("user.dir")
      val path = tempDir + "/warehouse"
      val spark = SparkSession.builder()
                              .appName("ProcessSensorDataWithSparkStream")
                              .master("local[*]")
                              .config("spark.sql.warehouse.dir", path)
                              //.config("spark.sql.streaming.checkpointLocation", "/Scala Workspace/AlertSystem/AlertSystemControl/checkpoint")
                              .getOrCreate()
                              
      val sourcePath= System.getProperty("user.dir").concat("\\streamsource").replace('\\', '/')
      val checkpointLoc1 = System.getProperty("user.dir").concat("\\checkpoint").concat("\\checkpoint1").replace('\\', '/')
      val checkpointLoc2 = System.getProperty("user.dir").concat("\\checkpoint").concat("\\checkpoint2").replace('\\', '/')
      val checkpointLoc3 = System.getProperty("user.dir").concat("\\checkpoint").concat("\\checkpoint3").replace('\\', '/')
      
      val stagePath_streamdata  = System.getProperty("user.dir").concat("\\streamstage").concat("\\streamdata").replace('\\', '/')
      val stagePath_extremedata = System.getProperty("user.dir").concat("\\streamstage").concat("\\extremedata").replace('\\', '/')
      val stagePath_missingdata = System.getProperty("user.dir").concat("\\streamstage").concat("\\missingdata").replace('\\', '/')
      
      spark.sparkContext.setLogLevel("ERROR")
      
      val schemaDef = new StructType()
                          .add (name="Date", StringType)
                          .add (name="Time", StringType)
                          .add (name="Tambient", DoubleType)
                          .add (name="Tcontainer_1", DoubleType)
                          .add (name="Tcontainer_2", DoubleType)
                          .add (name="Pyranometer_Gt_Row1", StringType)
                          .add (name="Pyranometer_Gt_Row2", StringType)
                          .add (name="Pyranometer_Gt_Row3", StringType)
                          .add (name="Pyranometer_Gt_Row4", StringType)
                          .add (name="Pyranometer_Gh", StringType)
                          .add (name="Humidity", DoubleType)
                          .add (name="APE", DoubleType)
                          .add (name="WD", DoubleType)
                          .add (name="WS", DoubleType)
      
      
      val streamingData = spark.readStream
                               .schema(schemaDef)
                               .option("header","true")
                               .csv(sourcePath)         
      
      streamingData.createOrReplaceTempView("sensor_data")
      
      val sensor_df= spark.sql("select "+
                                    "to_timestamp(unix_timestamp(concat(trim(Date), concat(' ',substr(trim(Time),0,5))), 'dd.MM.yyy HH:mm')) as date_time, "+
                                    "trim(Date) as date, "+
                                    "trim(Time) as time, "+
                                    "substr(trim(Time), 0,5) hhmm, "+
                                    "case when cast(substr(trim(Time),0,2) as int) < 12 then 'AM' else 'PM' end am_flag, "+
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
                                    "from sensor_data")
      sensor_df.createOrReplaceTempView("sensor_data")
      
      val sensor_extreme_values= spark.sql("select "+
                                            "date_time, "+
                                            "date, "+
                                            "time, "+
                                            "hhmm, "+
                                            "am_flag, "+
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
                                            "from sensor_data "+
                                            "where Tcontainer_2 > 35 OR Tcontainer_2 < 10")
                                            
                                                  
      val sensor_missing_values= spark.sql("select "+
                                            "date_time, "+
                                            "date, "+
                                            "time, "+
                                            "hhmm, "+
                                            "am_flag, "+
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
                                            "from sensor_data "+
                                            "where Tambient is null or "+
                                            "      Tcontainer_1 is null or "+
                                            "      Tcontainer_2 is null or "+
                                            "      Pyranometer_Gt_Row1 is null or "+
                                            "      Pyranometer_Gt_Row2 is null or "+
                                            "      Pyranometer_Gt_Row3 is null or "+
                                            "      Pyranometer_Gt_Row4 is null or "+
                                            "      Pyranometer_Gh is null or "+
                                            "      Humidity is null or "+
                                            "      APE is null or "+
                                            "      WD is null or "+ 
                                            "      WS is null")
                                    
      
                                            
      val stream_console = sensor_df.writeStream
                                    .format("console")
                                    .outputMode(OutputMode.Append())
                                    .start()     
                                    
      val streamstage_streamdata = sensor_df.writeStream
                                            .format("csv")
                                          //.trigger(Trigger.ProcessingTime("5 seconds"))
                                            .option("checkpointLocation", checkpointLoc1)
                                            .option("path", stagePath_streamdata)
                                            .start()        
      
                                    
      val streamstage_extremedata = sensor_extreme_values.writeStream
                                                         .format("csv")
                                                       //.trigger(Trigger.ProcessingTime("5 seconds"))
                                                         .option("checkpointLocation", checkpointLoc2)
                                                         .option("path", stagePath_extremedata)
                                                         .outputMode(OutputMode.Append())
                                                         .start()
                                                         
      
                                                         
      val streamstage_missingdata = sensor_missing_values.writeStream
                                                         .format("csv")
                                                       //.trigger(Trigger.ProcessingTime("5 seconds"))
                                                         .option("checkpointLocation", checkpointLoc3)
                                                         .option("path", stagePath_missingdata)
                                                         .outputMode(OutputMode.Append())
                                                         .start()         
      
      
      if (streamstage_missingdata.isActive) {
         sendEmail.sendMail(subject="Alert - Missing Data", 
                            text="There are missing values streaming from source. Please check missingdata stage path")
      } 
      
      if (streamstage_extremedata.isActive) {
         sendEmail.sendMail(subject="Alert - Extreme Data", 
                            text="Sensors incoming data exceeded or dropped from configured threshold. Please check extremedata stage path")
      } 
                                                         
      stream_console.awaitTermination()                         
      streamstage_streamdata.awaitTermination()
      streamstage_missingdata.awaitTermination()
      streamstage_extremedata.awaitTermination()
      
  }
  
}