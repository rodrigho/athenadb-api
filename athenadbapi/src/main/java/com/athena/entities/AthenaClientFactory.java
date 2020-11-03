package com.athena.entities;

import com.athena.util.Util;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;

public class AthenaClientFactory {

    private String accessKeyId;
    private String secretKey;
    private String region;
    private Boolean useEc2InstanceCredentials;

    private AwsCredentialsProvider awsCredentials;
    private AthenaClientBuilder builder;

    public AthenaClientFactory(String accessKeyId, String secretKey, String region, Boolean useEc2InstanceCredentials){
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.region = region;
        this.useEc2InstanceCredentials = useEc2InstanceCredentials;
    }

    public AthenaClient createClient() {
        awsCredentials = getAthenaInstanceProvider();

        builder = AthenaClient.builder()
                .region(Util.getRegion(region))
                .credentialsProvider(awsCredentials);
        return builder.build();
    }

    private AwsCredentialsProvider getAthenaInstanceProvider() {
        return useEc2InstanceCredentials ? InstanceProfileCredentialsProvider.create() : () -> new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return accessKeyId;
            }

            @Override
            public String secretAccessKey() {
                return secretKey;
            }
        };
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
}
