package com.distributed.assignment1;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Objects;

public class Manager {

    public static int fileParser(String filePath , String queue, SqsClient sqs){
        int taskCount=0;
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))){
            String line;
            while((line = reader.readLine()) != null){
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(queue).messageBody(line).build());
                taskCount++;
            }
        }catch(IOException e){
            System.out.println("File-not-found-manager");
        }
        return taskCount;
    }

    public static int countWorkers(Ec2Client ec2 ){
        int count = 0;
        Filter nameFilter = Filter.builder()
                .name("tag:Name")
                .values("Worker")
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
                count ++;
            }
        }
        return count; // number of current workers
    }

    public static void createWorkers (Ec2Client ec2 , int n){
        while(n>0) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T1_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(/*your USER DATA script string*/.getBytes()))
                .build();

            RunInstancesResponse response = ec2.runInstances(runRequest); // asks AWS to launch a new EC2 instance
            String instanceId = response.instances().get(0).instanceId();
            Tag tag = Tag.builder().key("Name").value("Worker").build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder().resources(instanceId).tags(tag).build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf("Successfully started EC2 instance Worker based on AMI %s", instanceId, amiId);
                n--;

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }


    public static void main(String[] args) {
        boolean termianteReq = false;
        software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
        Ec2Client ec2 = Ec2Client.builder().region(region).build(); // in order to talk to the EC2 services we need EC@ client here as well
        SqsClient sqs = SqsClient.builder().region(region).build();
        S3Client s3 = S3Client.builder().region(region).build();

        GetQueueUrlResponse q = sqs.getQueueUrl(
                GetQueueUrlRequest.builder().queueName("LocalToManagerQueue").build() // the queue name was created on the localApp side and we recieve its address.
        );
        String requestQueueUrl = q.queueUrl();
        CreateQueueRequest queueReqMTW = CreateQueueRequest.builder().queueName("ManagerToWorker").build();
        CreateQueueResponse queueMTL = sqs.createQueue(queueReqMTW);// creating the queue
        String manager2workerURL = queueMTL.queueUrl(); // unique URL to indetify the queue.

        while(termianteReq == false){
            ReceiveMessageResponse res = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(requestQueueUrl).maxNumberOfMessages(1) //The receipt handle is like a delete token for an SQS message.
                    .waitTimeSeconds(10).build()
            ); // cretaing the request how to pull from the queue
            if(!res.hasMessages()){
               //loop logic - todo
            }
            String bodyOfM = res.messages().get(0).body(); // the first messege of the queue; also add handaling for terminate.
            String [] parts = bodyOfM.split(" ");
            if(Objects.equals(parts[0], "FileInBucket")) {
                String bucket = parts[1];
                String fileName = parts[2];
                int limitOfWorkers = Integer.parseInt(parts[3]);

                Path localInputPath = Paths.get("managerInput.txt"); // what was sent from the localApp as input file will be saved as managerInput.txt
                s3.getObject(GetObjectRequest.builder().bucket(bucket).key(fileName).build(),localInputPath); // saved the inputFile into managerInput.
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(requestQueueUrl).receiptHandle(res.messages().get(0).receiptHandle()).build()); //message delete
                int taskCount = fileParser("managerInput.txt", manager2workerURL,sqs);
                int workersAtMoment = countWorkers(ec2);
                int missingWorkers = (taskCount + limitOfWorkers -1)/limitOfWorkers; // numbers of lines in the file / limit per worker (rounded up).
                int toLaunch = Math.max(0, missingWorkers - workersAtMoment);
                toLaunch = Math.min(toLaunch, 17 - workersAtMoment);// limit is 19 but we will do 17 for any case
                createWorkers(ec2 , toLaunch);
            }
            else{
                termianteReq = true;
                //terminate logic - todo
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(requestQueueUrl).receiptHandle(res.messages().get(0).receiptHandle()).build());
            }
        }
    }
}
//response
// └── reservations[]
//       └── instances[] so each reservation can have more then one instance
