/**
 * Created by Paul Yang on 2017/4/15.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

public class simpleRddMain {

    //Used to sum
    static int countSum = 0;

    public static void main(String[] args) {

        //parallel a RDD
        ArrayList<Integer> intList = new ArrayList<Integer>(){{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }};
        SparkConf conf = new SparkConf().setAppName("simple RDD opt").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> integerRdd = sc.parallelize(intList); // Get a RDD from a list.
        System.out.println("Integer RDD:");
        integerRdd.collect();

        //Lambda expressions
        JavaRDD<String> stringRdd = sc.textFile("/home/paul/spark/spark-2.1.0-bin-hadoop2.7/README.md");
        JavaRDD<Integer> intLineLength = stringRdd.map(s -> s.length());
        intLineLength.persist(StorageLevel.MEMORY_ONLY());
        int totalLen = intLineLength.reduce((a, b) -> a + b);
        System.out.println("Lines(" + stringRdd.count() + ")<<<Lambda expressions>>>: Total len = " + totalLen);

        //anonymous inner class or a name one
        class GetLenFunc implements Function<String, Integer> {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }
        JavaRDD<Integer> funcLineLengths = stringRdd.map( new GetLenFunc() );
        int funcTotalLen = funcLineLengths.reduce( new Function2<Integer, Integer, Integer>() {
           public Integer call (Integer a, Integer b) {return a + b;}
        });
        System.out.println("<<<anonymous inner class or a name one>>>: Total Len = " + funcTotalLen);


        //Wordcount Process
        JavaRDD<String> wordsRdd = stringRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList( line.split(" ")).iterator();
            }
        });
        //JavaRDD<String> words = stringRdd.flatMap(s -> s.split(" ").iterator());
        JavaPairRDD<String, Integer> eachWordRdd = wordsRdd.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> wordCntRdd = eachWordRdd.reduceByKey( (a, b) -> a + b );
        wordCntRdd.collect();
        wordCntRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "@@@" + stringIntegerTuple2._2);
            }
        });

        //Understanding closures
        integerRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                countSum += integer.intValue();
            }
        });
        System.out.println("#~~~~~scope and life cycle of variables and methods~~~~~~# countSum = " + countSum);

        //Working with Key-Value Pairs
        JavaPairRDD<String, Integer> strIntPairRdd = stringRdd.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> strCountRdd = strIntPairRdd.reduceByKey((a, b) -> a + b);
        //strCountRdd.sortByKey();
        strCountRdd.collect();
        System.out.println("###Working with Key-Value Pairs### :" + strCountRdd.toString());
        strCountRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ":" + stringIntegerTuple2._2);
            }
        });

        //Broadcast Variables
        Broadcast<double[]> broadcastVar = sc.broadcast(new double[] {1.1, 2.2, 3.3});
        broadcastVar.value();
    }
}
