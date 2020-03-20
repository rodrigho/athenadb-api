package com.athena.services;

import com.athena.entities.Config;
import com.athena.entities.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class QueryService {

    private static final Logger logger = LoggerFactory.getLogger(MainService.class);

    public QueryService(){}

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
}
