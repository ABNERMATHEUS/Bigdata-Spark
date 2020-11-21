package Question5;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Question5 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("realEstate").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //CARREGANDO ARQUIVO
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        int POSICAO_COUNTRY = 0; //posicao do vetor
        int POSICAO_UNIT = 7;
        int POSICAO_YEAR = 1;
        int POSICAO_CATEGORY = 9;
        int POSICAO_PRICE = 5;

        //removendo primeira linha e trazendo somente BRAZIL
        linhas = linhas.filter(l-> !l.startsWith("country_or_area") && l.split(";")[POSICAO_COUNTRY].equals("Brazil"));

        //criar estrutura no formato <chave,valor> (chave = Unit,valor=(year, category))
        JavaPairRDD<String, AvgCount> prdd = linhas.mapToPair(l -> {
            String[] colunas = l.split(";");

            String unit = colunas[POSICAO_UNIT];
            String year = colunas[POSICAO_YEAR];
            String categoria = colunas[POSICAO_CATEGORY];
            double price = Double.parseDouble(colunas[POSICAO_PRICE]);
            return new Tuple2<>(("Categoria=" + categoria + " unit=" + unit + " Ano= " + year), new AvgCount(1, price));
        });

        JavaPairRDD<String, AvgCount> somado = prdd.reduceByKey((x, y) ->
                new AvgCount(x.getCount() + y.getCount(),
                        x.getPrice() + y.getPrice()));

        // dando print dos resultados parciais
        somado.foreach(v -> System.out.println(v._1() + "\t" + v._2()));

        // 2a etapa: divido o preco total das casas pela quantidade de casas
//        JavaPairRDD<Integer, Double> prddMedia =
//                somado.mapToPair(i -> new Tuple2<>(i._1(), i._2().getPrecoCasas() / i._2().getQtdCasas()));

        JavaPairRDD<String, Double> prddMedia = somado.mapValues(x -> x.getPrice() / x.getCount());

        // ordenar
        JavaPairRDD<String, Double> ordenado = prddMedia.sortByKey(true);

        // salvar os resultados / utilizando coalesce para os resultado vim em um unico arquivo
        ordenado.coalesce(1).saveAsTextFile("output/media_commodities.txt");

    }

}
