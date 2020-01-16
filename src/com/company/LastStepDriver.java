package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.rrd4j.graph.DownSampler;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.text.SimpleDateFormat;

import static org.apache.spark.sql.functions.*;

public class LastStepDriver {
    public static void main(String[] args) throws IOException {
        Path path = new Path("output\\Last-Step");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("LastStep")
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


        Dataset<Row> temp = dataset_submis
                .groupBy("user_id")
                .agg(max("timestamp"))
                .join(dataset_submis,"user_id");


        Dataset<Row> temset = temp.where("max(timestamp) == timestamp")
                .groupBy("step_id")
                .count()
                .sort("count");
        temset.repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/Last-Step");


        sparkSession.stop();
    }
}
