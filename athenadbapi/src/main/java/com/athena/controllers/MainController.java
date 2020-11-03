package com.athena.controllers;

import com.athena.entities.Config;
import com.athena.entities.Response;
import com.athena.services.MainService;
//import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
//import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
public class MainController {

    private final MainService mainService;
    private ExecutorService nonBlockingService = Executors.newCachedThreadPool();

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

    @PostMapping(value = "test-connection", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> testConnection(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.testConnection(config));
    }

    @PostMapping(value = "execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> getQueryResult(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.runQuery(config));
    }

    @PostMapping(value = "stop", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Response> stopQueryId(@RequestBody Config config) {
        return ResponseEntity.ok(mainService.stopQueryId(config));
    }

    /*@PostMapping(value = "execute-stream", consumes = MediaType.APPLICATION_JSON_VALUE)
    public SseEmitter handleSse(@RequestBody Config config) {
        SseEmitter emitter = new SseEmitter();
        nonBlockingService.execute(() -> {
            try {

                Response res = mainService.runQueryStream(config, emitter);

                ObjectMapper mapper = new ObjectMapper();
                //Converting the Object to JSONString
                String jsonString = mapper.writeValueAsString(res);

                emitter.send(jsonString);
                // we could send more events

                emitter.complete();
            } catch (Exception ex) {
                emitter.completeWithError(ex);
            }
        });
        return emitter;
    }*/
}
