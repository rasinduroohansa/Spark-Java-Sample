package com.example.spark.sparkdemo;

import com.example.spark.sparkdemo.model.CommonData;
import org.apache.spark.sql.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@EnableAutoConfiguration
@EnableScheduling
@Configuration
public class TestSpark implements ApplicationListener<ContextRefreshedEvent> {
    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        System.out.println("Staring .....");
        runJsonDatasetExample();
    }
    private static void runJsonDatasetExample() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> interval = spark.read().option("multiline","true").json("./src/main/resources/opportunityIntervalData.json");
        interval.printSchema();
        interval.createOrReplaceTempView("interval");

        Dataset<Row> namesDF = spark.sql("SELECT " +
                "rowId," +
                "(dCArrayOutput/(1-(11/100))) dcArrayOutputIncludingOldPanel," +
                " bround(Min_by(1-((cellTemperature-25)*(-(35/10000))),1), 0)*100 efficiencyFromOldPanel," +
                "bround(Min_by(1-((cellTemperature-25)*(-(34/10000))),1), 0)*100 efficiencyFromNewPanel  FROM interval " +
                "GROUP By rowId,dcArrayOutputIncludingOldPanel");
        namesDF.createOrReplaceTempView("initialData");
        Encoder<CommonData> commonDataEncoder = Encoders.bean(CommonData.class);
        Dataset<CommonData> commonDataForAllYears = spark.sql("SELECT rowId," +
                "dcArrayOutputIncludingOldPanel," +
                "efficiencyFromOldPanel," +
                "(dcArrayOutputIncludingOldPanel/efficiencyFromOldPanel)* 100  dcArrayOutputExcludingOldPanel, efficiencyFromNewPanel," +
                "(efficiencyFromNewPanel* ((dcArrayOutputIncludingOldPanel/efficiencyFromOldPanel)* 100))/100 dcArrayOutputIncludingNewPanel from initialData ORDER BY rowId ASC").as(commonDataEncoder);
//        namesDF.createOrReplaceTempView("finalInterval");
//        Dataset<Row> finalOutput = spark.sql("select * from finalInterval");
        commonDataForAllYears.show();
        System.out.println("Done");
    }
}
