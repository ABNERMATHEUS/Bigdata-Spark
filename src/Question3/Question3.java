package Question3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Question3 {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR); //Não deixa os erros aparecerem
        SparkConf conf = new SparkConf().setAppName("questao3").setMaster("local[1]"); //Criar app e o setMater habilita o uso de uma thread
        JavaSparkContext sc = new JavaSparkContext(conf);       //configuração

        JavaRDD<String> linhas = sc.textFile("in/transactions.csv"); //pegar os valores

        int POSICAO_ANO = 1; //posicao do vetor
        int type = 4;   //posicao do vetor
        int codeCommodity = 2; //posicao do vetor

        linhas = linhas.filter(l-> l.split(";")[POSICAO_ANO].equals("2016")); //filtra as linhas

        JavaRDD<String> mapeamento = linhas.flatMap( l-> Arrays.asList(l.split(";")[type]+"\t"+l.split(";")[codeCommodity]).iterator()); //mapear - minha chave é um vetor

        JavaPairRDD<String,Integer> chave_valor = mapeamento.mapToPair( p ->  new Tuple2<>(p,1));  //chave valor

        JavaPairRDD<String,Integer> Reduce = chave_valor.reduceByKey((x,y) -> x+y);

        List<Tuple2<String,Integer>> resultado = Reduce.collect();

        for(Tuple2<String,Integer> r: resultado){
            System.out.println(r._1()+" = "+r._2());
        }

        Reduce.coalesce(1).saveAsTextFile("output/Question3.txt"); //Salvar o arquivo - coalesce é utilizado para salvar em uma partição
















    }
}
