package com.distributed.assignment1;

import java.io.File;
import java.util.Base64;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class LocalApplication {
    // function in order to check if there is a manager instance already
    private static String findActiveManager(Ec2Client ec2) {
        Filter nameFilter = Filter.builder()
                .name("tag:Name")
                .values("Manager")
                .build();

        Filter stateFilter = Filter.builder()
                .name("instance-state-name")
                .values("pending", "running")
                .build();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder() //filter request to check for manager instances with pending or running status not terminated
                .filters(nameFilter, stateFilter)
                .build();

        DescribeInstancesResponse response = ec2.describeInstances(request); // all the instances of the correct type

        for (Reservation res : response.reservations()) {
            for (Instance inst : res.instances()) {
                return inst.instanceId(); // return first active Manager
            }
        }
        return null; // no Manager found
    }

    public static String CreateManager (Ec2Client ec2){
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T1_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(/*your USER DATA script string*/.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest); // asks AWS to launch a new EC2 instance
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("Name")
                .value("Manager")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance manager based on AMI %s",
                    instanceId, amiId);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Wrong input, please provide - an  input a file with URLs of text files together with the required text analysis,\n" +
                    " an integer n stating how many files a single worker can analyze,\n" +
                    "and an optional argument which indicates the behaviour in case the local application sends a\n" +
                    "terminate message to the Manager.");
            System.exit(1);
        }
        String filePath = args[0];
        int Filelimit = Integer.parseInt(args[1]); // an integer stating how many files a single worker can analyze
        //args[3]
        File URLFile = new File(filePath);
        if (!URLFile.exists()) {
            System.out.println("File not found: " + filePath);
            System.exit(1);
        }

        Ec2Client ec2 = Ec2Client.create(); //creates a Java object (a client) that lets the program talk to the EC2 service
        String managerId = findActiveManager(ec2);
        if(managerId == null) {
            managerId = CreateManager(ec2);
        }

        S3Client s3 = S3Client.builder().region(Region.US_WEST_2).build(); // check region
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket("MishaBucketFromBGU").build()); // bucket name needs to be unique
        }catch (S3Exception e) {
            if (!e.awsErrorDetails().errorCode().equals("BucketAlreadyOwnedByYou")) {
                throw e;  // real error → rethrow
            }
            // else: bucket already exists → ignore
        }
        s3.putObject(PutObjectRequest.builder().bucket("MishaBucketFromBGU").key("inputFile").build(), URLFile.toPath());
    }
}

// we cant create two buckets because the second one will crush so we handle this with try catch.