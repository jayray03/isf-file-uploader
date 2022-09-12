package com.kroger.ifs.repository.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Log4j2
public abstract class S3Repository {

	protected AmazonS3 s3Client;

	public void createBucket(@NonNull String bucketName) {
		try {
			s3Client.createBucket(bucketName);
		} catch (SdkClientException ex) {
			log.error(ex);
		}
	}

	public void uploadObject(@NonNull String bucketName, @NonNull String key, String content) {
		try {
			s3Client.putObject(bucketName, key, content);
		} catch (SdkClientException ex) {
			log.error(ex);
		}
	}

	public void uploadObject(@NonNull String bucketName, @NonNull String key, File content) {
		try {
			s3Client.putObject(bucketName, key, content);
		} catch (SdkClientException ex) {
			log.error(ex);
		}
	}

	public void uploadObject(@NonNull String bucketName, @NonNull String key, byte[] content) {

		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(content.length);
		PutObjectRequest request = new PutObjectRequest(bucketName, key, new ByteArrayInputStream(content), objectMetadata);
		try {
			s3Client.putObject(request);
		} catch (SdkClientException ex) {
			log.error(ex);
		}
	}

	public void uploadObject(@NonNull String bucketName, @NonNull String key, InputStream content, ObjectMetadata objectMetadata) {
		PutObjectRequest request = new PutObjectRequest(bucketName, key, content, objectMetadata);
		s3Client.putObject(request);
	}

	public List<String> getAllBucketNames() {
		List<String> bucketNames = new ArrayList<>();
		List<Bucket> buckets = new ArrayList<>();

		try {
			buckets = s3Client.listBuckets();
		} catch (SdkClientException ex) {
			log.error(ex);
		}

		buckets.forEach(bucket -> bucketNames.add(bucket.getName()));

		return bucketNames;
	}

	public byte[] getS3Object(@NonNull String bucketName, @NonNull String key) throws IOException {
		S3Object s3Object;

		try {
			s3Object = s3Client.getObject(bucketName, key);
		} catch (SdkClientException ex) {
			log.error(ex);
			return null;
		}

		return IOUtils.toByteArray(s3Object.getObjectContent());
	}

	public S3ObjectInputStream getS3ObjectInputStream(@NonNull String bucketName, @NonNull String key) {
		S3Object s3Object;

		try {
			s3Object = s3Client.getObject(bucketName, key);
		} catch (SdkClientException ex) {
			log.error(ex);
			return null;
		}

		return s3Object.getObjectContent();
	}

	public List<S3ObjectSummary> getObjectListByBucketName(String bucketName) {
		ListObjectsV2Result result;

		try {
			result = s3Client.listObjectsV2(bucketName);
		} catch (SdkClientException ex) {
			log.error(ex);
			return null;
		}

		return result.getObjectSummaries();
	}

	/*
	 * Multipart Upload Methods
	 * TODO - We are currently not using this method of uploading, when we try to upload with multipart upload, the
	 *  file is created but has 0 bytes.  Look into this issue if we need to use multipart upload later.
	 */
	public String startMultipartUpload(@NonNull String bucketName, @NonNull String key) {

		InitiateMultipartUploadRequest multipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, key);
		InitiateMultipartUploadResult multipartUploadResult = s3Client.initiateMultipartUpload(multipartUploadRequest);
		return multipartUploadResult.getUploadId();
	}

	public PartETag uploadMultipart(@NonNull String bucketName, @NonNull String key, @NonNull String uploadId,
									@NonNull Integer partNumber, InputStream inputStream) throws IOException {

		ObjectMetadata objectMetadata = new ObjectMetadata();

		UploadPartRequest uploadPartRequest = new UploadPartRequest()
				.withBucketName(bucketName)
				.withKey(key)
				.withUploadId(uploadId)
				.withPartNumber(partNumber)
				.withInputStream(inputStream)
				.withObjectMetadata(objectMetadata);

		return s3Client.uploadPart(uploadPartRequest).getPartETag();
	}

	public void completeMultipartUpload(@NonNull String bucketName, @NonNull String key, @NonNull String uploadId,
										@NonNull List<PartETag> completedParts) {

		CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName
				, key, uploadId, completedParts);

		s3Client.completeMultipartUpload(completeMultipartUploadRequest);

	}
}
