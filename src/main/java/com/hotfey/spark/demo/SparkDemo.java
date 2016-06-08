/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hotfey.spark.demo;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author pipe
 */
public class SparkDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkDemo").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = javaSparkContext.textFile("README.md");
        System.out.println(textFile.count());
        System.out.println(textFile.first());

        JavaRDD<String> linesWithSpark = textFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("Spark");
            }
        });
        System.out.println(linesWithSpark.count());

        Integer lineWithMostWords = textFile.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.split(" ").length;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return Math.max(a, b);
            }
        });
        System.out.println(lineWithMostWords);

        JavaPairRDD<String, Integer> wordCounts;
        wordCounts = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(final String s) throws Exception {
                return new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                };
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        System.out.println(wordCounts.collect());
    }
}
