package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;
import java.util.stream.Stream;

public class UnicUserDriver {

    public static void main(String[] args) throws IOException {

        Path path = new Path("output\\unic_users");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Unic-user-driver").setMaster("local")
        );


        sparkContext
                .textFile("input/logs_example.csv")
                .map(line -> line.split(",")[4])
                .distinct()
                .saveAsTextFile("output/unic_users");

        sparkContext.stop();
    }

}

