package com.kinesis.poc.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;

@Configuration
public class BeanInitializer {

    @Bean
    public AmazonKinesis buildAmazonKinesis() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("accessKey", "secretKey");
        return AmazonKinesisClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(Regions.EU_CENTRAL_1)
                .build();
    }

}
