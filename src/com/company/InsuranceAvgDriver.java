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

public class InsuranceAvgDriver {
    public static void main(String[] args) throws IOException {

        Path path = new Path("output\\Insurance-average");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Insurance")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/insurance.csv");
        dataset.select("insurer_nm","claim_id","invoice_date","total_hrs_in_status")
                .withColumn("total_hrs_in_status",new Column("total_hrs_in_status").cast(DataTypes.IntegerType))
                .groupBy("insurer_nm","claim_id","invoice_date")
                .sum("total_hrs_in_status")
                .withColumnRenamed("sum(total_hrs_in_status)","hrs")
                .select("insurer_nm","claim_id","invoice_date","hrs")
                .groupBy("insurer_nm")
                .avg("hrs")
                .withColumnRenamed("avg(hrs)","average hrs per request")
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/Insurance-average");
    }
}
