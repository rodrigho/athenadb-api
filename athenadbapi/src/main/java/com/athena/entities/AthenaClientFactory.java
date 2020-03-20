package com.athena.entities;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.athena.util.Util;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;

public class AthenaClientFactory {

    private String accessKeyId;
    private String secretKey;
    private String region;

    private AwsCredentialsProvider awsCredentials;
    private AthenaClientBuilder builder;

    public AthenaClientFactory(String accessKeyId, String secretKey, String region){
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.region = region;
    }

    public AthenaClient createClient() {
        awsCredentials = () -> new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return accessKeyId;
            }

            @Override
            public String secretAccessKey() {
                return secretKey;
            }
        };

        builder = AthenaClient.builder()
                .region(Util.getRegion(region))
                .credentialsProvider(awsCredentials);
        return builder.build();
    }

    public AWSCredentialsProvider getAwsCredentials(){
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        System.out.println(accessKeyId);
                        return accessKeyId;
                    }

                    @Override
                    public String getAWSSecretKey() {
                        System.out.println(secretKey);
                        return secretKey;
                    }
                };
            }

            @Override
            public void refresh() {
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
