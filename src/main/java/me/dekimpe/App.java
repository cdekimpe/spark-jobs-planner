package me.dekimpe;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
        SparkConf conf = new SparkConf()
                .set("spark.executor.extraClassPath", "/home/hadoop/*:")
                .setAppName("Spark Jobs Planner")
                .set("spark.executor.instances", "2")
                .set("spark.executor.cores", "1")
                .setMaster("spark://192.168.10.14:7077");
        
        JavaSparkContext sc = new JavaSparkContext(conf);

        int hour = 25;
        Date date;
        while(true) {
            date = new Date();
            System.out.println(date);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int currentHour = cal.get(Calendar.HOUR_OF_DAY);
            if(hour != currentHour) {
                System.out.println("Launching new 'Spark Tweets - Top 10'...");
                SparkLauncher sparkLauncher = new SparkLauncher()
                        .setMaster("spark://192.168.10.14:7077")
                        .setMainClass("me.dekimpe.App")
                        .addAppArgs("/home/hadoop/tweets-spark-top-10/target/tweets-spark-top-10-1.0-SNAPSHOT-jar-with-dependencies.jar")
                        .addSparkArg("--packages", "org.apache.spark:spark-avro_2.11:2.4.3");
                hour = currentHour;
            }
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
