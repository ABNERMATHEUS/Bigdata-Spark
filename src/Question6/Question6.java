package Question6;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Question6 {
    public static void main(String[] args) {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("highestPriceUnitYear").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Carregar o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        //The commodity with the highest price per unit type and year;

        //removendo primeira linha e trazendo somente BRAZIL
        linhas = linhas.filter(l-> !l.startsWith("country_or_area"));

        //criar estrutura no formato <chave,valor> (chave = Unit,valor=(year, category))
        JavaPairRDD<String, Double> prdd = linhas.mapToPair(l -> {
            String[] colunas = l.split(";");

            String unit = colunas[7];
            String year = colunas[1];
            String commodity = colunas[3];
            double price = Double.parseDouble(colunas[5]);
            return new Tuple2<>(("Commodity= " + commodity + "Unit= " + unit + " Year= " + year), price);
        });

        JavaPairRDD<String, Double> maxPrice = prdd.reduceByKey((v1, v2) -> Math.max(v1, v2)); // deixar apenas v1 + v2

        // ordenar
        JavaPairRDD<String, Double> ordenado = maxPrice.sortByKey(false);

        ordenado.coalesce(1).saveAsTextFile("output/Question6.txt");
    }
}