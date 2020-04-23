package com.athena.controllers;

import com.athena.entities.Config;
import com.athena.entities.Response;
import com.athena.services.MainService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class MainController {

    private final MainService mainService;

    @Autowired
    public MainController(MainService mainService) {
        this.mainService = mainService;
    }

    @GetMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getHome() {
        return ResponseEntity.ok(mainService.getSalute());
    }

    @GetMapping(value = "salute", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getSalute() {
        return ResponseEntity.ok(mainService.getSalute());
    }

    @PostMapping(value = "test-connection", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> testConnection(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.testConnection(config));
    }

    @PostMapping(value = "execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> getQueryResult(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.runQuery(config));
    }

    @PostMapping(value = "s3", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<String>> getS3Files(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.getS3File(config));
    }

    @PostMapping(value = "stop", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> stopQueryId(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.stopQueryId(config));
    }
}
