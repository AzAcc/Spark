package com.company;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.stream.Stream;

public class WordCountDriver {

    public static void main(String[] args) {
        File dir = new File("output\\word-count");

        File[] listFiles = dir.listFiles();
        for(File file : listFiles){
            file.delete();
        }
        dir.delete();
        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );

        sparkContext
                .textFile("input/doc1.txt")
                .flatMap(s -> Stream.of(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a1, a2) -> a1 + a2)
                .saveAsTextFile("output/word-count");

        sparkContext.stop();
    }

}
