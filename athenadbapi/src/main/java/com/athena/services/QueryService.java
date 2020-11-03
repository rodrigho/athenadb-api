package com.athena.services;

import com.athena.entities.Config;
import com.athena.entities.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;


@Component
public class QueryService {

    private static final Logger logger = LoggerFactory.getLogger(MainService.class);

    public QueryService(){}

    public Response testConnection(Config config){
        Response response = new Response();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion(),
                config.getUseEc2InstanceCredentials());
        try {
            response = athenaService.testConnection(config);
        } catch (Exception e) {
            response.setStatus("FAILED");
            response.setMessage(e.toString());
            logger.error(e.toString());
        }
        return response;
    }

    public Response runQuery(Config config){
        Response response = new Response();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion(),
                config.getUseEc2InstanceCredentials());
        try {
            response = athenaService.executeQuery(config);
        } catch (Exception e) {
            response.setStatus("FAILED");
            response.setMessage(e.toString());
            logger.error(e.toString());
        }
        return response;
    }

    public Response runQueryStream(Config config, SseEmitter emitter){
        Response response = new Response();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion(),
                config.getUseEc2InstanceCredentials());
        try {
            response = athenaService.executeQueryStream(config, emitter);
        } catch (Exception e) {
            response.setStatus("FAILED");
            response.setMessage(e.toString());
            logger.error(e.toString());
        }
        return response;
    }

    public Response stopQuery(Config config){
        Response response = new Response();
        AthenaService athenaService = new AthenaService(
                config.getAthenaDatabase(),
                config.getAthenaOutputBucket(),
                config.getTimeSleep(),
                config.getAccessKeyId(),
                config.getSecretKey(),
                config.getRegion(),
                config.getUseEc2InstanceCredentials());
        try {
            response = athenaService.stopQueryExecution(config);
        } catch (Exception e) {
            response.setStatus("FAILED");
            response.setMessage(e.toString());
            logger.error(e.toString());
        }
        return response;
    }

}
