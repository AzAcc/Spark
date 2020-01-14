package com.company;

import java.io.File;

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

import static org.apache.spark.sql.functions.desc;

public class PopularVideoDriver {

    public static void main(String[] args) throws Exception {

        Path path = new Path("output\\Popular-Videos");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Top-Videos")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/RUvideos.csv");
        dataset.select("title","views")
                .withColumn("views",new Column("views").cast(DataTypes.IntegerType))
                .sort(desc("views"))
                .distinct()
                .limit(10)
                .repartition(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save("output/Popular-Videos");
        sparkSession.stop();
    }
}
