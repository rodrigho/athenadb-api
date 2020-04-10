package com.athena.services;

import com.athena.entities.Config;
import com.athena.entities.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class QueryService {

    private static final Logger logger = LoggerFactory.getLogger(MainService.class);

    public QueryService(){}

    public Response testConnection(Config config){
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion());

        return athenaService.testConnection();
    }

    public Response runQuery(Config config){
        Response response = new Response();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion());

        try {
            response = athenaService.executeQuery(config);
        } catch (InterruptedException e) {
            response.setStatus("FAILED");
            response.setMessage(e.toString());
            logger.error(e.toString());
        }
        return response;
    }

    public List<String> getS3Files(Config config){
        List<String> list = new ArrayList<>();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion());

        try {
            list = athenaService.cleanS3Directory("mmm-athena-poc","s3://mmm-athena-poc/phoenix-results/", config.getRegion());
        } catch (Exception e) {
            logger.error(e.toString());
        }
        return list;
    }
}
