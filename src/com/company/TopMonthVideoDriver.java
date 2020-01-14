package com.company;

import java.io.File;
import java.text.SimpleDateFormat;

import javassist.runtime.Desc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;

import static org.apache.spark.sql.functions.*;

public class TopMonthVideoDriver {

    public static void main(String[] args) throws Exception {

        Path path = new Path("output\\Top-Month");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Top-Month")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/RUvideos.csv");

        dataset.select("title","views","trending_date")
                .withColumn("views",new Column("views").cast(DataTypes.IntegerType))
                .withColumn("temp_date",new Column("trending_date").substr(7,8))
                .withColumn("trending_date",new Column("trending_date").substr(1,3))
                .withColumn("trend_year_month",concat(new Column("trending_date"),(new Column("temp_date"))))
                .drop("temp_date","trending_date")
                .where("trend_year_month like \"__.__\"")
                .groupBy(new Column("trend_year_month"))
                .agg(max("views").alias(dataset.col("title").toString()))
                .repartition(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save("output/Top-Month");

        sparkSession.stop();
    }
}
