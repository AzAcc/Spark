package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import parquet.hadoop.util.counters.ICounter;
import scala.Tuple4;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class SuspiciousUsersDriver {

    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Path path = new Path("output\\Susp-Users");
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
                .filter(line -> line.contains("LOGIN"))
                .map(line -> line.split(","))
                .map(line -> new Tuple4<>(line[2],line[3],line[4],sdf.parse(line[5]).getTime()))
                .groupBy(Tuple4::_1)
                .map(line -> line._2)
                .saveAsTextFile("output/Susp-Users");
        sparkContext.stop();

    }

}

