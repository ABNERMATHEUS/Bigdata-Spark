package Question1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Question1 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("questao01").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        int POSICAO_PAIS = 0;

        JavaRDD<String> reddBrazil = linhas.filter(a -> a.split(";")[POSICAO_PAIS].equals("Brazil"));
        long countTransactions = reddBrazil.count();

        System.out.println("Transactions involving Brazil: " + countTransactions);
    }




}
