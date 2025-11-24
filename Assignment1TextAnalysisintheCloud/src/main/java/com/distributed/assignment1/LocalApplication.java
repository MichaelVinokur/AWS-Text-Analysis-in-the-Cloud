package com.distributed.assignment1;

import java.io.File;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.UUID;


import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import software.amazon.awssdk.regions.Region;


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

    public static String createManager (Ec2Client ec2){
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
        int Filelimit = Integer.parseInt(args[1]); // (n) an integer stating how many files a single worker can analyze
        boolean terminateMode = (args.length == 3 && args[2].equalsIgnoreCase("terminate"));
        File inputFile = new File(filePath);
        if (!inputFile.exists()) {
            System.out.println("File not found: " + filePath);
            System.exit(1);
        }
        software.amazon.awssdk.regions.Region region = Region.US_EAST_1; // check region
        String bucketName = "MishaBucketFromBGU";

        Ec2Client ec2 = Ec2Client.builder().region(region).build(); //creates a Java object (a client) that lets the program talk to the EC2 service
        String managerId = findActiveManager(ec2);
        if(managerId == null) {
            managerId = createManager(ec2);
        }

        S3Client s3 = S3Client.builder().region(region).build();
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()); // bucket name needs to be unique and have unique name or else crash
        }catch (S3Exception e) {
            if (!e.awsErrorDetails().errorCode().equals("BucketAlreadyOwnedByYou")) {
                throw e;  // real error → rethrow
            }
            // else: bucket already exists → ignore
        }
        s3.putObject(PutObjectRequest.builder().bucket(bucketName).key("inputFile").build(), inputFile.toPath()); // creates S3 input of the file with value - bucketname and key.

        SqsClient sqs = SqsClient.builder().region(region).build();
        //inputQueue
        CreateQueueRequest queueReqMTL = CreateQueueRequest.builder().queueName("ManagerToLocalQueue-" + UUID.randomUUID()).build(); // so the manager will know to which local to talk
        CreateQueueResponse queueMTL = sqs.createQueue(queueReqMTL);// creating the queue
        String returnQueueUrl = queueMTL.queueUrl(); // unique URL to indetify the queue.
        //outputQueue
        CreateQueueRequest queueReqLTM = CreateQueueRequest.builder().queueName("LocalToManagerQueue").build(); // requst to create LocalToManagerQueue
        CreateQueueResponse queueLTM = sqs.createQueue(queueReqLTM);// creating the queue
        String queueUrl = queueLTM.queueUrl(); // unique URL to indetify the queue.
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody( "NEW_TASK " + bucketName + " " + "inputFile" + " " + Filelimit + " " + returnQueueUrl).build());
        while(true){
            ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(returnQueueUrl)
                    .maxNumberOfMessages(1).waitTimeSeconds(10).build()); //waiting to recieve from the manager the end file

            if(!response.messages().isEmpty()){
                String[] parts = response.messages().get(0).body().split(" ");
                String outBucket = parts[0];
                String outKey = parts[1];
                s3.getObject(GetObjectRequest.builder().bucket(outBucket).key(outKey).build(), Paths.get("output.html"));// downloads the object from s3 + saves the file to the computer
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(returnQueueUrl).receiptHandle(response.messages().get(0).receiptHandle()).build());
                break;
            }
            try {
                Thread.sleep(3000); // maybe not relvent
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (terminateMode) {// send a TERMINATE command to the manager
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("TERMINATE").build());
        }
    }
}

// we cant create two buckets because the second one will crush so we handle this with try catch.
// also we create 2 queus to handle the situation of more then one localApp so that in case of one queue any local app can read messeges meant for diffrenet LocalApp (even with 2 queues we need to handle this case even more deeply)
//queus are better for the meneger to be able to read from one specific queue. and of course the case where the Local App reads its own massage which is bad.
