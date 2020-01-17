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

public class TestDriver {
    public static void main(String[] args) throws IOException {
        Path path = new Path("output\\TestWrong");
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
        Dataset<Row> tempset = dataset_submis.select("user_id","timestamp")
                .withColumn("timestamp",new Column("timestamp").cast(DataTypes.LongType))
                .groupBy("user_id")
                .agg(max("timestamp"))
                .withColumnRenamed("max(timestamp)","max_time")
                .withColumnRenamed("user_id","tempset_user_id");

        Dataset<Row> answer1 = dataset_submis.join(tempset,dataset_submis.col("user_id").equalTo(tempset.col("tempset_user_id")).and(
                dataset_submis.col("timestamp").equalTo(tempset.col("max_time"))),"inner")
                .where("submission_status == \"wrong\"")
                .select("user_id","step_id");
        answer1.repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/TestWrong");
        answer1.groupBy("step_id").count().orderBy(col("count").desc()).show(1);

        sparkSession.stop();
    }
}
