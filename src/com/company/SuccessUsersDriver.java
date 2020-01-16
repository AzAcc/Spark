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

public class SuccessUsersDriver {
    public static void main(String[] args) throws IOException {
        Path path = new Path("output\\Suc-User");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        Path path2 = new Path("output\\Suc-Steps");
        Configuration conf2 = new Configuration();
        FileSystem fs2 = FileSystem.get(conf2);
        if (fs2.exists(path2)) {
            fs2.delete(path2, true);
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
                .load("input/event_data_train.csv");
        Dataset<Row> datasetu = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/submissions_data_train.csv");

       long count_steps = dataset.select("step_id").distinct().count();

       Dataset<Row> dataset1 = dataset.select("step_id","action","user_id").where(dataset.col("action").like("passed"))
                .groupBy("user_id")
                .count();
       Dataset<Row> dataset2 = dataset1.select("user_id","count")
               .where(String.format("count= %d",count_steps))
               .drop("count");
       dataset2.repartition(1)
               .write()
               .format("csv")
               .option("header",true)
               .save("output/Suc-User");

       datasetu.select("step_id","submission_status","user_id")
               .distinct()
               .where(datasetu.col("submission_status").like("correct"))
                .groupBy("user_id")
                .count()
                .join(dataset2,"user_id")
               .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("output/Suc-Steps");


        sparkSession.stop();
    }
}
