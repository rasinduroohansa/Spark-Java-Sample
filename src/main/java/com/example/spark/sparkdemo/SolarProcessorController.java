package com.example.spark.sparkdemo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.StringUtils;
import com.example.spark.sparkdemo.model.CommonData;
import com.example.spark.sparkdemo.model.SolarInputDataset;
import com.example.spark.sparkdemo.request.SolarInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@RestController
public class SolarProcessorController {
    @RequestMapping(method = RequestMethod.POST, path = "/initiateSolarEngine")
    public String count(@RequestBody SolarInput input) {
        try {
            generateSolarTechEngine(input);
            return "Done";
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public void generateSolarTechEngine(SolarInput input){
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        SparkSession spark = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .master("local[*]")
                .getOrCreate();

        try {
            long start = System.currentTimeMillis();
            Duration duration = Duration.ofMillis(System.currentTimeMillis() - start);
            /**
             * Reading Data file from S3 and push it into spark as a string.
             */
//            S3Object o = s3.getObject(input.getBucketName(), input.getPath());
//            S3ObjectInputStream s3is = o.getObjectContent();
//            String str = getAsString(s3is);
//            List<String> data = Arrays.asList(str);
//            Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
//            Dataset<Row> interval = spark.read().json(ds);

            /**
             * Reading Local stored Json file from Spark.
             */
//            Dataset<Row> interval = spark.read().option("multiline", "true").json("./src/main/resources/opportunityIntervalData.json");
//            interval.printSchema();

            /**
             * Directly accessing the S3 stored file from spark hadoop using s3a file system.
             */
            JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
            /**
             * Needs to provide aws credential provider in to spark hadoop configuration to build
             * aws credential provider. If we are not using default credential provide we have to
             * send fs.s3a.access.key and fs.s3a.secret.key in to the hadoop configuration to access
             * aws account.
             */
            sparkContext.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
            start = System.currentTimeMillis();
            Dataset<Row> interval = spark.read().option("multiline", "true").json("s3a://lean-energy-upload-dev/spark/opportunityIntervalData.json");
            duration = Duration.ofMillis(System.currentTimeMillis() - start);
            System.out.println("Reading S3 File => " + duration);
            start = System.currentTimeMillis();

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
                    "(efficiencyFromNewPanel* ((dcArrayOutputIncludingOldPanel/efficiencyFromOldPanel)* 100))/100 dcArrayOutputIncludingNewPanel " +
                    "from initialData ORDER BY rowId ASC").as(commonDataEncoder);
//        namesDF.createOrReplaceTempView("finalInterval");

            /**
             * Saving Spark output in to local file system as a json file
             */
//            commonDataForAllYears.write().json("./src/main/output/");
            /**
             * Uploading processed json file into s3 using s3 put object method.
             */
            ObjectMapper mapper = new ObjectMapper();
            s3.putObject(input.getBucketName(),"spark/direct_output/result.json", mapper.writeValueAsString(commonDataForAllYears.collectAsList()));
            /**
             * Upload spark output as a set of chunk
             */
//            commonDataForAllYears.write().json("s3a://lean-energy-upload-dev/spark/output/");
            /**
             * Upload spark output as a single json file
             */
//            commonDataForAllYears.coalesce(1).write()
//                    .format("json")
//                    .save("s3a://lean-energy-upload-dev/spark/outputAsOne/");

            duration = Duration.ofMillis(System.currentTimeMillis() - start);
            System.out.println("Result Uploaded to S3 => " + duration);
        }catch (Exception e){
            System.out.println(e);
        }
    }

    @RequestMapping(method = RequestMethod.POST, path = "/getSolarInput")
    public List<SolarInputDataset> gets3File(@RequestBody SolarInput input) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        try {
            S3Object o = s3.getObject(input.getBucketName(), input.getPath());
            S3ObjectInputStream s3is = o.getObjectContent();
            String str = getAsString(s3is);
            ObjectMapper mapper = new ObjectMapper();
            List<SolarInputDataset> inputList = mapper.readValue(str, new TypeReference<List<SolarInputDataset>>(){});
            return inputList;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    private static String getAsString(InputStream is) throws IOException {
        if (is == null)
            return "";
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StringUtils.UTF8));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            is.close();
        }
        return sb.toString();
    }
}

