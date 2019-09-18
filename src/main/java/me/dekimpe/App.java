package me.dekimpe;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
        int hour = 0;
        Date date;
        while(true) {
            date = new Date();
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int currentHour = cal.get(Calendar.HOUR_OF_DAY);
            if(hour != currentHour) {
                SparkLauncher sparkLauncher = new SparkLauncher()
                        .addFile("/home/hadoop/tweets-spark-top-10/target/tweets-spark-top-10-1.0-SNAPSHOT-jar-with-dependencies.jar")
                        .addSparkArg("--packages", "org.apache.spark:spark-avro_2.11:2.4.3");
            }
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
