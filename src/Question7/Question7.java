package Question7;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Question7 {

    public static void main(String[] args) {

        // Omite erros
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Set nome aplicação
        SparkConf conf = new SparkConf().setAppName("realEstate").setMaster("local[*]");

        // Padrão de injeção
        JavaSparkContext sc = new JavaSparkContext(conf);

        // CARREGANDO ARQUIVO
        JavaRDD<String> rows = sc.textFile("in/transactions.csv");

        final int FLOW_INDEX = 4;
        final int YEAR_INDEX = 1;

        // Removendo primeira linha
        rows = rows.filter(l-> !l.startsWith("country_or_area"));

        // Mapeamento de chaves baseada no ano e tipo de flow
        JavaRDD<String> yearAndFlowKeys = rows.flatMap(l -> Arrays.asList(l.split(";")[YEAR_INDEX] + "\t" + l.split(";")[FLOW_INDEX]).iterator());

        // Criação de tupla baseado em pares
        JavaPairRDD<String, Integer> mapping = yearAndFlowKeys.mapToPair(m -> new Tuple2<>(m, 1));

        // Executo a soma de chaves, agrupo
        JavaPairRDD<String, Integer> reduce = mapping.reduceByKey((x, y) -> x + y);

        // Converto em lista
        List<Tuple2<String, Integer>> result = reduce.collect();

        System.out.println("Resultado:\n");
        for (Tuple2<String, Integer> valor : result)
        {
            System.out.println("Tupla: " + valor._1() + "-- Total de transações: " + valor._2());
        }

        reduce.coalesce(1).saveAsTextFile("output/Question7.txt");

    }
}
