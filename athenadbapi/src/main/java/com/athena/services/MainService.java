package com.athena.services;

import com.athena.entities.Config;
import com.athena.entities.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
}
