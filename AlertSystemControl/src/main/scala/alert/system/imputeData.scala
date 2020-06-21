package alert.system
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object imputeData {
  
   def main (args:Array[String]) {
     
      val tempDir = System.getProperty("user.dir")
      val path = tempDir + "/warehouse"
      val spark = SparkSession.builder()
                              .appName("ImputeDataToTarget")
                              .master("local[*]")
                              .config("spark.sql.warehouse.dir", path)
                              .getOrCreate()
      
      spark.sparkContext.setLogLevel("ERROR")
      
      val stagePath_streamdata   = System.getProperty("user.dir").concat("\\streamstage\\streamdata")
      val stagePath_missingdata  = System.getProperty("user.dir").concat("\\streamstage\\missingdata")
      val stagePath_extremedata  = System.getProperty("user.dir").concat("\\streamstage\\extremedata")
      val targetPath_streamdata  = System.getProperty("user.dir").concat("\\streamtarget\\streamdata")
      val targetPath_missingdata = System.getProperty("user.dir").concat("\\streamtarget\\missingdata")
      val targetPath_extremedata = System.getProperty("user.dir").concat("\\streamtarget\\extremedata")
      
      
      val schemaDef = new StructType()
                          .add (name="date_time", TimestampType)
                          .add (name="date", StringType)
                          .add (name="time", StringType)
                          .add (name="hhmm", StringType)
                          .add (name="am_flag", StringType)
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
     
      while (true) {                    
        //reading full incoming stream data from streamdata stage path                    
        val streamdata = spark.read
                           .format("csv")
                           .schema(schemaDef)
                           .csv(stagePath_streamdata)
        streamdata.createOrReplaceTempView("sensor_data_stg") 
        
        //reading full incoming missing data from missing stage path                    
        val missingdata = spark.read
                           .format("csv")
                           .schema(schemaDef)
                           .csv(stagePath_missingdata)
        
        //reading full incoming missing data from missing stage path                    
        val extremedata = spark.read
                           .format("csv")
                           .schema(schemaDef)
                           .csv(stagePath_extremedata)
        
        val impute_by_lag = spark.sql (
            "select "+
            "date_time, "+
            "date, "+
            "time, "+
            "hhmm, "+
            "am_flag, "+
            "case when Tambient is null then lag (Tambient) over (order by date_time) "+
            "     else Tambient end as Tambient, "+
            "case when Tcontainer_1 is null then lag (Tcontainer_1) over (order by date_time) "+
            "     else Tcontainer_1 end as Tcontainer_1, "+
            "case when Tcontainer_2 is null then lag (Tcontainer_2) over (order by date_time) "+
            "     else Tcontainer_2 end as Tcontainer_2, "+
            "case when Pyranometer_Gt_Row1 is null then lag (Pyranometer_Gt_Row1) over (order by date_time) "+
            "     else Pyranometer_Gt_Row1 end as Pyranometer_Gt_Row1, "+
            "case when Pyranometer_Gt_Row2 is null then lag (Pyranometer_Gt_Row2) over (order by date_time) "+
            "     else Pyranometer_Gt_Row2 end as Pyranometer_Gt_Row2, "+
            "case when Pyranometer_Gt_Row3 is null then lag (Pyranometer_Gt_Row3) over (order by date_time) "+
            "     else Pyranometer_Gt_Row3 end as Pyranometer_Gt_Row3, "+
            "case when Pyranometer_Gt_Row4 is null then lag (Pyranometer_Gt_Row4) over (order by date_time) "+
            "     else Pyranometer_Gt_Row4 end as Pyranometer_Gt_Row4, "+
            "case when Pyranometer_Gh is null then lag (Pyranometer_Gh) over (order by date_time) "+
            "     else Pyranometer_Gh end as Pyranometer_Gh, "+
            "case when Humidity is null then lag (Humidity) over (order by date_time) "+
            "     else Humidity end as Humidity, "+
            "case when APE is null then lag (APE) over (order by date_time) "+
            "     else APE end as APE, "+
            "case when WD is null then lag (WD) over (order by date_time) "+
            "     else WD end as WD, "+
            "case when WS is null then lag (WS) over (order by date_time) "+
            "     else WS end as WS "+
            "from sensor_data_stg "+
            "order by date_time"
        )
        
        impute_by_lag.createOrReplaceTempView("dataset_impute1")
        
        val extract_remaining_nulls = spark.sql (
            "select * from dataset_impute1 "+
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
            "      WS is null"
        )
        
        //extract_remaining_nulls.show(10)
        
        //writing imputed data to the target folder
        impute_by_lag.repartition(1)
                     .write
                     .option("header", "true")
                     .mode("overwrite")
                     .csv(targetPath_streamdata)
                     
                     
        //writing consolidated missing data to target missing data folder
        missingdata.repartition(1)
                     .write
                     .option("header", "true")
                     .mode("overwrite")
                     .csv(targetPath_missingdata)
                     
        //writing consolidated extreme data to target missing data folder
        extremedata.repartition(1)
                     .write
                     .option("header", "true")
                     .mode("overwrite")
                     .csv(targetPath_extremedata)
        
        //Make the current thread sleep for 10 seconds. 
        //So every 10 seconds target will be refreshed based on whatever data available at stage to replicate a near real time target refresh
        println("Target write completed. Sleep for 10 seconds")
        Thread.sleep(10000)  
        
        println("Target re-write to bring in new data")
      }
   }
}