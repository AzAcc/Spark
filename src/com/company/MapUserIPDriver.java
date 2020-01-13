package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;

public class MapUserIPDriver {

    public static void main(String[] args) throws IOException {

        Path path = new Path("output\\User-IP");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("User-IP-Driver").setMaster("local")
        );

        sparkContext
                .textFile("input/logs_example.csv")
                .map(line -> line.split(","))
                .mapToPair(line -> new Tuple2<>(line[4], line[2]))
                .distinct().groupByKey()
                .saveAsTextFile("output/User-IP");

        sparkContext.stop();
    }

}

