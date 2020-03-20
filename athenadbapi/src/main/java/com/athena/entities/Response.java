package com.athena.entities;

import java.util.List;

public class Response {
    private String queryExecutionId;
    private List<List<String>> lists;
    private String status;
    private String message;
    private long duration;

    public Response(){}

    public Response(String queryExecutionId, List<List<String>> lists, String status, String message, long duration) {
        this.queryExecutionId = queryExecutionId;
        this.lists = lists;
        this.status = status;
        this.message = message;
        this.duration = duration;
    }

    public String getQueryExecutionId() {
        return queryExecutionId;
    }

    public void setQueryExecutionId(String queryExecutionId) {
        this.queryExecutionId = queryExecutionId;
    }

    public List<List<String>> getLists() {
        return lists;
    }

    public void setLists(List<List<String>> lists) {
        this.lists = lists;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}
