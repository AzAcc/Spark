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

public class LastWrongDriver {
    public static void main(String[] args) throws IOException {
        Path path = new Path("output\\LastWrong");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("LastChance")
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


        Dataset<Row>temp_correct = dataset_submis.where("submission_status == \"correct\"")
                .groupBy("user_id")
                .agg(max("timestamp"))
                .withColumnRenamed("max(timestamp)","correct_max");

        Dataset<Row>temp = dataset_submis.where("submission_status == \"wrong\"")
                .groupBy("user_id")
                .agg(max("timestamp"))
                .withColumnRenamed("max(timestamp)","wrong_max")
                .join(temp_correct,"user_id");

        Dataset<Row>temp_wrong = dataset_submis.where("submission_status == \"wrong\"");

        temp.where("wrong_max > correct_max")
                .join(temp_wrong,"user_id")
                .groupBy("user_id")
                .agg(last("step_id"))
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/LastWrong");


        sparkSession.stop();
    }
}
