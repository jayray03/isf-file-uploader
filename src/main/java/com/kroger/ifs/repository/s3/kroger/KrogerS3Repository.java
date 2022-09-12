package com.kroger.ifs.repository.s3.kroger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.kroger.ifs.repository.s3.S3Repository;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class KrogerS3Repository extends S3Repository {

	public KrogerS3Repository(@Value("${kroger.aws.s3.access.key}") String accessKey,
							  @Value("${kroger.aws.s3.secret.key}") String secretKey,
							  @Value("${kroger.aws.s3.region}") String region,
							  @Value("${kroger.aws.s3.endpoint}") String s3endpoint) {

		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

		super.setS3Client(AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3endpoint, region))
				.enablePathStyleAccess()
				.build()
		);
	}
}
