package com.athena.services;

import com.athena.entities.Config;
import com.athena.entities.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;


@Service
public class MainService {

    private final QueryService queryService;

    @Autowired
    public MainService(QueryService queryService) {
        this.queryService = queryService;
    }

    public String getSalute() {
        return "Hello World!";
    }

    public synchronized Response runQuery(Config config) {
        return queryService.runQuery(config);
    }

    public synchronized Response runQueryStream(Config config, SseEmitter emitter) {
        return queryService.runQueryStream(config, emitter);
    }

    public synchronized Response testConnection(Config config){
        return queryService.testConnection(config);
    }


    public synchronized Response stopQueryId(Config config) {
        return queryService.stopQuery(config);
    }
}
