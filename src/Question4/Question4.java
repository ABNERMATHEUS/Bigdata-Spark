package Question4;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Question4 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);  //Tratamento do erro
        SparkConf conf = new SparkConf().setAppName("questao4").setMaster("local[1]"); //Ele fazer somente em 1 Thread
        JavaSparkContext sc = new JavaSparkContext(conf); //Adicionar configuração

        JavaRDD<String> linhas = sc.textFile("in/transactions.csv"); //pegar valor

        int TRADE_USD_POSITION = 5; //posição do valor do commodity
        int YEAR_POSITION = 1; //posição do ano

        linhas = linhas.filter(l-> !l.startsWith("country_or_area")); //removendo o cabeçalho

        JavaPairRDD<String,AvgCount> prdd =  linhas.mapToPair(x->{   //Criar estrutura para chave,valor
            String[] colunas = x.split(";");
            String valor = colunas[TRADE_USD_POSITION];
            String chave = colunas[YEAR_POSITION]; //ano

            return new Tuple2<>(chave,new AvgCount(Double.parseDouble(valor),1));
        });

        JavaPairRDD<String,AvgCount> somando = prdd.reduceByKey((x,y)-> new AvgCount(x.getValor()+y.getValor(),x.getN()+ y.getN())); //reduzindo minhas chaves em 1 para cada chave

        JavaPairRDD<String,Double> prddMedia = somando.mapValues(x-> x.getValor()/ x.getN()); //irá pegar cada um da chave e valor reduzindo em 1 chave já conforme foi feito em cima e tirar a média

        for(Tuple2<String,Double> r: prddMedia.collect()){
            System.out.println(r._1()+"\t"+r._2()+" %");
        }

        prddMedia.coalesce(1).saveAsTextFile("output/Question4.txt");

    }
}
