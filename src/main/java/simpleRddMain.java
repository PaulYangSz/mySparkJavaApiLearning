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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class simpleRddMain {

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
        JavaRDD<Integer> integRdd = sc.parallelize(intList); // Get a RDD from a list.
        System.out.println("Double RDD:");
        integRdd.collect();

        //Lambda expressions
        JavaRDD<String> stringRdd = sc.textFile("/home/paul/spark/spark-2.1.0-bin-hadoop2.7/README.md");
        JavaRDD<Integer> intLineLength = stringRdd.map(s -> s.length());
        intLineLength.persist(StorageLevel.MEMORY_ONLY());
        int totalLen = intLineLength.reduce((a, b) -> a + b);
        System.out.println("<<<Lambda expressions>>>: Total len = " + totalLen);

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

        stringRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return null;
            }
        });

        //Understanding closures
        int[] countSum = {0};
        integRdd.foreach(x -> countSum[0] += x);
        System.out.println("#scope and life cycle of variables and methods# countSum[0] = " + countSum[0]);

        //Working with Key-Value Pairs
        JavaPairRDD<String, Integer> strIntPairRdd = stringRdd.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> strCountRdd = strIntPairRdd.reduceByKey((a, b) -> a + b);
        strCountRdd.sortByKey();
        strCountRdd.collect();
        System.out.println("###Working with Key-Value Pairs### :" + strCountRdd.toString());
        strCountRdd.foreach(println);
        //JavaRDD<String> wordRdd = stringRdd.flatMap(line -> line.split(" "));
    }
}
