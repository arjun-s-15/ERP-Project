package com.ERP.Invoice_MS.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Files;
import java.nio.file.Path;

@Service
public class S3UploadService {

    private final S3Client s3Client;

    @Value("${aws.bucketName}")
    private String bucketName;

    public S3UploadService(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void uploadFile(String key, Path filePath) {
        System.out.println("=== S3 Upload Debug ===");
        System.out.println("Bucket: " + bucketName);
        System.out.println("Key: " + key);
        System.out.println("File exists: " + Files.exists(filePath));

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try {
            s3Client.putObject(request, filePath);
            System.out.println("Upload successful: " + key);
        } catch (Exception e) {
            System.err.println("Upload failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}