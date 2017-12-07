/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import java.io.*;
import java.lang.String;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.File;

public class SimpleApp {
  public static void main(String[] args) {
    
     long debut =  System.currentTimeMillis();
     String logFile = "/root/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
     SparkConf sparkconf = new  SparkConf().setAppName("Locality");
     JavaSparkContext contx = new JavaSparkContext(sparkconf);
         //int val =1; 
         //String recup="";
       JavaRDD<String> textFile = contx.textFile("onlyBayLocsUsersWithFreq.txt");

       JavaPairRDD<String , Integer>   counts = textFile
    .flatMap(s -> Arrays.asList(s.substring(s.indexOf(" ")+1).split(" ")).iterator())
    .mapToPair(word -> new Tuple2<>(word.substring(0,word.length()-2), 1))
    .reduceByKey((a, b) -> a + b);
counts.saveAsTextFile("fichier3.txt");


     }
}

