package com.athena.services;

import com.athena.entities.AthenaClientFactory;
import com.athena.entities.Config;
import com.athena.entities.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import software.amazon.awssdk.core.SdkField;
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
                                  String accessKeyId, String secretKey, String region, Boolean useEc2InstanceCredentials){
        this.athenaDatabase = athenaDatabase;
        this.athenaOutputBucket = athenaOutputBucket;
        this.timeSleep = timeSleep;
        this.status = "";
        this.athenaClientFactory = new AthenaClientFactory(accessKeyId, secretKey, region, useEc2InstanceCredentials);
    }

    public Response testConnection(Config config){
        Response response;
        long startTime = System.currentTimeMillis();
        AthenaClient athenaClient = athenaClientFactory.createClient();
        String message = "";
        try{
            ListNamedQueriesResponse list = athenaClient.listNamedQueries();
            status = "SUCCEEDED";
            System.out.println(status + " " + list);
        }catch (Exception ex){
            message = ex.toString();
            status = "FAILED";
            logger.error(status);
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

    public Response executeQuery(Config config) {
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

    public Response executeQueryStream(Config config, SseEmitter emitter) {
        Response response;
        long startTime = System.currentTimeMillis();
        String message = "";
        List<List<String>> lists = new ArrayList<>();
        String queryExecutionId = config.getQueryExecutionId();

        try {
            AthenaClient athenaClient = athenaClientFactory.createClient();

            if (!config.isUseQueryId())
                queryExecutionId = submitAthenaQuery(athenaClient, config.getQueries().get(0));

            emitter.send(queryExecutionId);

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
            System.out.println(rowValues);
            rows.add(rowValues);
        }
        return rows;
    }

    public Response stopQueryExecution(Config config){
        Response response;
        long startTime = System.currentTimeMillis();
        String message = "";
        List<List<String>> lists = new ArrayList<>();
        String queryExecutionId = config.getQueryExecutionId();
        AthenaClient athenaClient = athenaClientFactory.createClient();
        try {
            // Submit the stop query Request
            StopQueryExecutionRequest stopQueryExecutionRequest = StopQueryExecutionRequest.builder()
                    .queryExecutionId(config.getQueryExecutionId()).build();

            StopQueryExecutionResponse stopQueryExecutionResponse = athenaClient.stopQueryExecution(stopQueryExecutionRequest);

            // Ensure that the query was stopped
            GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                    .queryExecutionId(config.getQueryExecutionId()).build();

            GetQueryExecutionResponse getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            if (getQueryExecutionResponse.queryExecution()
                    .status()
                    .state()
                    .equals(QueryExecutionState.CANCELLED)) {
                // Query was cancelled.
                System.out.println("Query has been cancelled");
            }
        }catch (Exception e){
            message = e.toString();
            logger.error(e.toString());
        }finally {
            long endTime = System.currentTimeMillis();
            response = new Response(queryExecutionId, lists, status, message, endTime - startTime);
        }
        return response;
    }

}
