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
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
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
//        JavaRDD<String> wordsRdd = stringRdd.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList( line.split(" ")).iterator();
//            }
//        });
        JavaRDD<String> wordsRdd = stringRdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
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

        //Accumulator
        LongAccumulator longAccum = sc.sc().longAccumulator();
        integerRdd.foreach(x -> longAccum.add(x));
        System.out.println("\n\n\nAccumulator: " + longAccum.value() + "\n\n\n\n");

        //AccumulatorV2
        class MyVector {
            double[] vals;

            public MyVector(int vecLen) {
                vals = new double[vecLen];
            }

            public void reset() {
                for(int i = 0; i < vals.length; i++) {
                    vals[i] = 0;
                }
            }

            public void add(MyVector inVec) {
                for(int i = 0; i < vals.length; i++) {
                    vals[i] += inVec.vals[i];
                }
            }
        }
        class VectorAccumulatorV2 extends AccumulatorV2<MyVector,MyVector> {
            private MyVector selfVect = null;

            public VectorAccumulatorV2(int vecLen) {
                selfVect = new MyVector(vecLen);
            }

            @Override
            public boolean isZero() {
                for(int i = 0; i < selfVect.vals.length; i++) {
                    if(selfVect.vals[i] != 0) return false;
                }
                return true;
            }

            @Override
            public AccumulatorV2<MyVector, MyVector> copy() {
                VectorAccumulatorV2 ret = new VectorAccumulatorV2(copy().value().vals.length);
                return ret;
            }

            @Override
            public void reset() {
                selfVect.reset();
            }

            @Override
            public void add(MyVector v) {
                selfVect.add(v);
            }

            @Override
            public void merge(AccumulatorV2<MyVector, MyVector> other) {
                MyVector minVec = null, maxVec = null;
                if(other.value().vals.length < selfVect.vals.length) {
                    minVec = other.value();
                    maxVec = selfVect;
                }
                else {
                    minVec = selfVect;
                    maxVec = other.value();
                }
                //TODO: merge together.
            }

            @Override
            public MyVector value() {
                return selfVect;
            }
        }
        VectorAccumulatorV2 myVecAcc = new VectorAccumulatorV2(5);
        sc.sc().register(myVecAcc, "MyVectorAcc1");


    }
}
