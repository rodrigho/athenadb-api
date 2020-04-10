package com.athena.services;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.athena.entities.AthenaClientFactory;
import com.athena.entities.Config;
import com.athena.entities.Response;
import com.athena.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class AthenaService {
    private String athenaDatabase;
    private String athenaOutputBucket;
    private long timeSleep;
    private String status;

    AthenaClientFactory athenaClientFactory;

    private static final Logger logger = LoggerFactory.getLogger(MainService.class);

    public AthenaService(String athenaDatabase, String athenaOutputBucket, long timeSleep,
                                  String accessKeyId, String secretKey, String region){
        this.athenaDatabase = athenaDatabase;
        this.athenaOutputBucket = athenaOutputBucket;
        this.timeSleep = timeSleep;
        this.status = "";
        this.athenaClientFactory = new AthenaClientFactory(accessKeyId, secretKey, region);
    }

    public Response testConnection(){
        Response response;
        long startTime = System.currentTimeMillis();
        String message = "";
        try{
            ListNamedQueriesResponse list = athenaClientFactory.createClient().listNamedQueries();
            status = "SUCCEEDED";
            System.out.println(list);
        }catch (Exception ex){
            message = ex.toString();
            status = "FAILED";
            logger.error(ex.toString());
        }finally {
            long endTime = System.currentTimeMillis();
            response = new Response("", null, status, message, endTime - startTime);
        }
        return response;
    }

    public void setAthenaDatabase(String athenaDatabase) {
        this.athenaDatabase = athenaDatabase;
    }

    public void setAthenaOutputBucket(String athenaOutputBucket) {
        this.athenaOutputBucket = athenaOutputBucket;
    }

    public void setTimeSleep(long timeSleep) {
        this.timeSleep = timeSleep;
    }

    public Response executeQuery(Config config) throws InterruptedException {
        Response response;
        long startTime = System.currentTimeMillis();
        String message = "";
        List<List<String>> lists = new ArrayList<>();
        String queryExecutionId = config.getQueryExecutionId();
        AthenaClient athenaClient = athenaClientFactory.createClient();
        try {
            if (!config.isUseQueryId())
                queryExecutionId = submitAthenaQuery(athenaClient, config.getQueries().get(0));

            logger.info("Query to test: " + queryExecutionId);
            waitForQueryToComplete(athenaClient, queryExecutionId);
            lists = processResultRows(athenaClient, queryExecutionId);

            formatResultForDescribe(lists);

            logger.info("Query finished");
        }catch (Exception e){
            message = e.toString();
            logger.error(e.toString());
        }finally {
            long endTime = System.currentTimeMillis();
            response = new Response(queryExecutionId, lists, status, message, endTime - startTime);
        }
        return response;
    }

    private void formatResultForDescribe(List<List<String>> lists){
        //verify if it is a desc
        if(lists.get(0).size() == 1 && lists.get(0).get(0).contains("\t")){
            HashSet<List<String>> set = new HashSet<>();
            for(List<String> list: lists){
                String word = list.get(0);
                if(!word.contains("# Partition Information") && !word.contains("# col_name") && !word.contains("\t \t ")){
                    String[] words = word.split("\t");
                    List<String> data = new ArrayList<>();
                    for (String w: words)
                        data.add(w.trim());
                    set.add(data);
                }
            }
            lists.clear();
            lists.add(0, Arrays.asList("col_name", "data_type", "comment"));
            lists.addAll(new ArrayList<>(set));
        }
    }

    public String submitAthenaQuery(AthenaClient athenaClient, String query) {

        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(athenaDatabase).build();

        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(athenaOutputBucket).build();

        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration).build();

        StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);

        return startQueryExecutionResponse.queryExecutionId();
    }

    private void waitForQueryToComplete(AthenaClient athenaClient, String queryExecutionId) throws InterruptedException {

        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;

        boolean isQueryStillRunning = true;

        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();

            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                Thread.sleep(timeSleep);
            }
            status = queryState;
            logger.info("Current Status is: " + queryState);
        }
    }

    public List<List<String>> processResultRows(AthenaClient athenaClient, String queryExecutionId) {
        List<List<String>> lists = new ArrayList<>();
        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

        for (GetQueryResultsResponse resultResult : getQueryResultsResults) {

            int resultSize = resultResult.resultSet().rows().size();
            logger.info("Result size: " + resultSize);

            List<SdkField<?>> aggs = resultResult.resultSet().sdkFields();
            for(SdkField f: aggs){
                System.out.println(f.toString());
            }

            List<Row> results = resultResult.resultSet().rows();
            lists.addAll(processRow(results));
        }
        return lists;
    }

    private List<List<String>> processRow(List<Row> rowList) {
        List<List<String>> rows = new ArrayList<>();

        for (Row row: rowList) {
            List<String> rowValues = new ArrayList<>();
            for (Datum datum : row.data()) {
                rowValues.add(datum.varCharValue());
            }
            rows.add(rowValues);
        }
        return rows;
    }

    public List<String> cleanS3Directory(String bucketName, String folderPath, String region){
        List<String> list = new ArrayList<>();
        try {
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(athenaClientFactory.getAwsCredentials())
                    .withRegion(Util.getRegions(region))
                    .build();

            System.out.println(s3.getS3AccountOwner());

            for (S3ObjectSummary file : s3.listObjects(bucketName, folderPath).getObjectSummaries()){
                logger.info(file.toString());
                list.add(file.getBucketName());
                //s3.deleteObject(bucketName, file.getKey());
            }
        } catch (AmazonServiceException e) {
            logger.error(e.toString());
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
        } catch (SdkClientException e) {
            logger.error(e.toString());
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
        }
        return list;
    }
}
