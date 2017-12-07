  GNU nano 2.5.3                 Fichier : SimpleAppquestion1.java                                       

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class SimpleApp {
  public static void main(String[] args) {
     long debut = System.currentTimeMillis();
     String logFile = "/root/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
     SparkConf sparkconf = new  SparkConf().setAppName("Integrale");
     JavaSparkContext contx = new JavaSparkContext(sparkconf);


       int a =1;
       int b=10;
       int nbr_rect=10000000;


       //calcul du pas 
       double pas = (1.0*(b-a)) / nbr_rect;

final int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 15;
                final int n = nbr_rect * slices;
                final List<Integer> l = new ArrayList<>(n);
                for (int i = 0; i < nbr_rect; i++) {
                        l.add(i);
                    }

                    final JavaRDD<Integer> dataSet = contx.parallelize(l, slices);


                           //calcul d'aire

                final double result = dataSet.map(i -> {
                      double aireRect =pas/(1+(i*pas));
                     return aireRect;
                }).reduce((c,d) -> c + d);

                          System.out.println(" le resutat de l'intégrale est:"+result);
                            long fin =  System.currentTimeMillis();
                            long  duree =  fin - debut ;

                          System.out.println("Le temps de calcul est: " +duree);

           }
}

