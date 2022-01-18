package com.n1k0.log_parser;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("SparkParser");
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        String inputPath;
        String searchSequence;
        String outputPath;

        if(args.length != 3){
            inputPath = "mobile_log";
            searchSequence = "+79999999999";
            outputPath = "filtered_mobile_log";
            System.out.println("No argument specified");
            System.out.println("arguments are set: log_parser mobile_log +79999999999 filtered_mobile_log");
        } else{
            inputPath = args[0];
            searchSequence = args[1];
            outputPath = args[2];
        }

        Dataset<String> inputDS = spark.read().textFile(inputPath);
        System.out.println("Read " + inputDS.count() + " string from " + inputPath);
        inputDS.show();

        Dataset<String> filteredDS = inputDS.filter(inputDS.col("value").contains(searchSequence));
        System.out.println("Find " + filteredDS.count() + " string from " + inputPath + " with " + searchSequence + " sequence");
        filteredDS.show();

        filteredDS.write().mode(SaveMode.Overwrite).text(outputPath);
        System.out.println("Save filtered string in " + outputPath);

    }
}
