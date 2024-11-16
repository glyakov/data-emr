package com.epam.training.data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.spark.sql.RowFactory;

import static org.apache.spark.sql.functions.col;

public class DataTrainingApplication {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                //.master("local[*]")
                .appName("S3 Access Logs Analyzer")
                .config("spark.ui.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        String inputPath = "s3a://yhliakau/emr/access_logs/*";
        String outputPath = "s3a://yhliakau/emr/traffic_reports/";

        Dataset<Row> logs = parseLogs(spark, inputPath);

        logs.write().mode(SaveMode.Overwrite).json(outputPath);
//        logs.show();
    }

    public static Dataset<Row> parseLogs(SparkSession spark, String logFilePath) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> logData = sc.textFile(logFilePath);

        JavaRDD<Row> rowRDD = logData.flatMap((FlatMapFunction<String, Row>) s -> {
            String regex = "(.*)  - (.*) \"(\\S+) (.*)\" (\\d{3}) \\[(.*)\\] \"(.*)\"";
            List<Row> list = new ArrayList<>();
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(s);
            if (matcher.find()) {
                list.add(RowFactory.create(
                        matcher.group(2), // service
                        matcher.group(3), // method
                        matcher.group(4), // uri
                        matcher.group(5), // status code
                        matcher.group(6), // timestamp
                        matcher.group(7)  // client
                ));
            }
            return list.iterator();
        });

        // Define schema
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("source", DataTypes.StringType, false),
                DataTypes.createStructField("method", DataTypes.StringType, false),
                DataTypes.createStructField("uri", DataTypes.StringType, false),
                DataTypes.createStructField("status_code", DataTypes.StringType, false),
                DataTypes.createStructField("timestamp", DataTypes.StringType, false),
                DataTypes.createStructField("client", DataTypes.StringType, false)
        });

        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema)
                .withColumn("filename", functions.input_file_name())
                .withColumn("target", functions.regexp_extract(col("filename"), "(\\w+-service)", 1))
                .withColumn("parsed_timestamp", col("timestamp").cast("timestamp"))
                .groupBy("source", "target")
                .count()
                .withColumn("id", functions.concat_ws("-", col("source"), col("target")))
                .withColumn("id", functions.hash(col("id")))
                .withColumnRenamed("count", "total_requests")
                .select(
                        col("id"),
                        col("source"),
                        col("target"),
                        col("total_requests")
                );
    }
}
