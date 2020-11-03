package com.athena.entities;

import java.util.List;

public class Config {

    private String athenaDatabase;
    private String athenaOutputBucket;
    private int timeSleep;
    private String accessKeyId;
    private String secretKey;
    private String region;
    private List<String> queries;
    private boolean useQueryId;
    private String queryExecutionId;
    private Boolean useEc2InstanceCredentials;

    public Config(){}

    public Config(String athenaDatabase, String athenaOutputBucket, int timeSleep, String accessKeyId, String secretKey,
                  String region, List<String> queries, boolean useQueryId, String queryExecutionId, Boolean useEc2InstanceCredentials) {
        this.athenaDatabase = athenaDatabase;
        this.athenaOutputBucket = athenaOutputBucket;
        this.timeSleep = timeSleep;
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.region = region;
        this.queries = queries;
        this.useQueryId = useQueryId;
        this.queryExecutionId = queryExecutionId;
        this.useEc2InstanceCredentials = useEc2InstanceCredentials;
    }

    public String getAthenaDatabase() {
        return athenaDatabase;
    }

    public void setAthenaDatabase(String athenaDatabase) {
        this.athenaDatabase = athenaDatabase;
    }

    public String getAthenaOutputBucket() {
        return athenaOutputBucket;
    }

    public void setAthenaOutputBucket(String athenaOutputBucket) {
        this.athenaOutputBucket = athenaOutputBucket;
    }

    public int getTimeSleep() {
        return timeSleep;
    }

    public void setTimeSleep(int timeSleep) {
        this.timeSleep = timeSleep;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<String> getQueries() {
        return queries;
    }

    public void setQueries(List<String> queries) {
        this.queries = queries;
    }

    public boolean isUseQueryId() {
        return useQueryId;
    }

    public void setUseQueryId(boolean useQueryId) {
        this.useQueryId = useQueryId;
    }

    public String getQueryExecutionId() {
        return queryExecutionId;
    }

    public void setQueryExecutionId(String queryExecutionId) {
        this.queryExecutionId = queryExecutionId;
    }

    public Boolean getUseEc2InstanceCredentials() {
        return useEc2InstanceCredentials;
    }

    public void setUseEc2InstanceCredentials(Boolean useEc2InstanceCredentials) {
        this.useEc2InstanceCredentials = useEc2InstanceCredentials;
    }

    @Override
    public String toString() {
        return "Config{" +
                "athenaDatabase='" + athenaDatabase + '\'' +
                ", athenaOutputBucket='" + athenaOutputBucket + '\'' +
                ", timeSleep=" + timeSleep +
                ", accessKeyId='" + accessKeyId + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", region='" + region + '\'' +
                ", queries=" + queries +
                ", useQueryId=" + useQueryId +
                ", queryExecutionId='" + queryExecutionId + '\'' +
                '}';
    }
}
