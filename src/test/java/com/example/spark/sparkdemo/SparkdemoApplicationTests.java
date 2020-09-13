package com.example.spark.sparkdemo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


@SpringBootTest
class SparkdemoApplicationTests {

    @Test
    private  void runJsonDatasetExample() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // $example on:json_dataset$
        // A JSON dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files
        Dataset<Row> people = spark.read().json("examples/src/main/resources/people.json");

        // The inferred schema can be visualized using the printSchema() method
        people.printSchema();
        // root
        //  |-- age: long (nullable = true)
        //  |-- name: string (nullable = true)

        // Creates a temporary view using the DataFrame
        people.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> namesDF = spark.sql("SELECT siteNetKWH FROM people");
        namesDF.show();
        // +------+
        // |  name|
        // +------+
        // |Justin|
        // +------+

        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // a Dataset<String> storing one JSON object per string.
//        List<String> jsonData = Arrays.asList(
//                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
//        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
//        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
//        anotherPeople.show();
        // +---------------+----+
        // |        address|name|
        // +---------------+----+
        // |[Columbus,Ohio]| Yin|
        // +---------------+----+
        // $example off:json_dataset$
        System.out.println("Done");
    }

}
