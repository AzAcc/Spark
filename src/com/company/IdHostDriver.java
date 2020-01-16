package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;

public class IdHostDriver {
    public static void main(String[] args) throws IOException {
        Path path = new Path("output\\IdHost");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SucSteps")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataset_submis = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/submissions_data_train.csv");
        Dataset<Row> dataset_event = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/event_data_train.csv");
        dataset_submis
                 .groupBy("user_id")
                 .count()
                .sort("count")
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/IdHost");


        sparkSession.stop();
    }
}
