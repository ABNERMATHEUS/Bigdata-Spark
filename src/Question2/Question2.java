package Question2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Question2 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("questao02").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        int POSICAO_ANO = 1;
        linhas.filter(l -> l.split(";")[POSICAO_ANO] != "year");
        JavaRDD<String> year = linhas.flatMap(l -> Arrays.asList(l.split(";")[POSICAO_ANO]).iterator());
        JavaPairRDD<String, Integer> perYear = year.mapToPair(p -> new Tuple2<>(p, 1));

        JavaPairRDD<String, Integer> ocorrencias = perYear.reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> resultado = ocorrencias.collect();
        for (Tuple2<String, Integer> r : resultado) {
            System.out.println(r._1() + "\t" + r._2());
        }




    }

}
