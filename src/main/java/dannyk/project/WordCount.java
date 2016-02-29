package dannyk.project;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount implements Serializable {
  
  public void simpleWordCount(JavaSparkContext sc, String filename) {
    JavaRDD<String> lines = sc.textFile(filename, 1);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) {
        return Arrays.asList(Pattern.compile(" ").split(s));
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    sc.stop();
  }
  
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
      System.exit(1);
    }

    SparkConf conf = new SparkConf();
    conf.setAppName("Java API demo");
    conf.setMaster(args[0]);
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    WordCount app = new WordCount();
    app.simpleWordCount(sc, args[1]);
  }
}
