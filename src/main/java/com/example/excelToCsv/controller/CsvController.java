package com.example.excelToCsv.controller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
public class CsvController {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();

    @PostMapping("/file")
    public ResponseEntity<String> file(@RequestParam("file") MultipartFile Webfile) throws IOException {

        byte[] bytes = Webfile.getBytes();
        Path path = Paths.get("/home/albanero/Documents/file.xlsx");
        Files.write(path, bytes);
        String file="/home/albanero/Documents/file.xlsx";


        //SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> df  = spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true").load(file);


        df.write().format("csv").option("header", "true").save("output.csv");
        spark.stop();
        return ResponseEntity.ok("================================= file received ====================");
    }

    @PostMapping("/headers")
    public ResponseEntity<String> headers(@RequestParam("name") String name, @RequestParam("rename") String rename) {
        Dataset<Row> df=spark.read().option("header", "true").csv("output.csv");
        df=df.withColumnRenamed(name,rename);
        df.show();
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
