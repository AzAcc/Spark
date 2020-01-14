package com.company;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SumEnergyUsageDriver {

    public static void main(String[] args) throws Exception {

        Path path = new Path("output\\Sum-Energy");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Sum-Energy")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/energy-usage-2010.csv");
        dataset.select("COMMUNITY AREA NAME","KWH JANUARY 2010",
                "KWH FEBRUARY 2010","KWH MARCH 2010","KWH APRIL 2010",
                "KWH MAY 2010","KWH JUNE 2010","KWH JULY 2010","KWH AUGUST 2010",
                "KWH SEPTEMBER 2010","KWH OCTOBER 2010","KWH NOVEMBER 2010","KWH DECEMBER 2010")
                .withColumn("KWH JANUARY 2010",new Column("KWH JANUARY 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH FEBRUARY 2010",new Column("KWH FEBRUARY 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH MARCH 2010",new Column("KWH MARCH 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH APRIL 2010",new Column("KWH APRIL 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH MAY 2010",new Column("KWH MAY 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH JUNE 2010",new Column("KWH JUNE 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH JULY 2010",new Column("KWH JULY 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH AUGUST 2010",new Column("KWH AUGUST 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH SEPTEMBER 2010",new Column("KWH SEPTEMBER 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH OCTOBER 2010",new Column("KWH OCTOBER 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH NOVEMBER 2010",new Column("KWH NOVEMBER 2010").cast(DataTypes.IntegerType))
                .withColumn("KWH DECEMBER 2010",new Column("KWH DECEMBER 2010").cast(DataTypes.IntegerType))
                .groupBy("COMMUNITY AREA NAME").sum("KWH JANUARY 2010",
                "KWH FEBRUARY 2010","KWH MARCH 2010","KWH APRIL 2010",
                "KWH MAY 2010","KWH JUNE 2010","KWH JULY 2010","KWH AUGUST 2010",
                "KWH SEPTEMBER 2010","KWH OCTOBER 2010","KWH NOVEMBER 2010","KWH DECEMBER 2010")
                .repartition(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save("output/Sum-Energy");
        sparkSession.stop();
    }
}
