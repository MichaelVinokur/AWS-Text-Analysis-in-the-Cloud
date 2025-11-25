package com.distributed.assignment1;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class Manager {

    private static final String amiId = "ami-0fa3fe0fa7920f68e";

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
        String workerUserData =
                "#!/bin/bash\n" +
                        "yum update -y\n" +
                        "yum install -y java-17-amazon-corretto-headless\n" +
                        "aws s3 cp s3://mtmv-bguproject/worker.jar /home/ec2-user/worker.jar\n" +
                        "java -jar /home/ec2-user/worker.jar > /home/ec2-user/worker.log 2>&1 &\n";
        while(n>0) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T1_MICRO)
                    .imageId(amiId)
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(workerUserData.getBytes()))
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

//    public static void createOutPut(int expected, String WTMurl, SqsClient sqs, S3Client s3, String bucket, String key){
//        int recieved = 0;
//        List<String> results = new ArrayList<>();
//        while( recieved < expected){
//            ReceiveMessageResponse res = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(WTMurl).maxNumberOfMessages(1) //The receipt handle is like a delete token for an SQS message.
//                    .waitTimeSeconds(5).build());
//            if (!res.hasMessages()) {
//                continue;
//            }
//            results.add(res.messages().get(0).body());
//            recieved++;
//            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(WTMurl).receiptHandle(res.messages().get(0).receiptHandle()).build());
//        }
//        Path summaryPath = Paths.get("summary.html");
//        try(BufferedWriter w = Files.newBufferedWriter(summaryPath)){
//            w.write("<html><body>\n");
//            for (String line : results) {
//                w.write(line);
//                w.write("<br/>\n");
//            }
//            w.write("</body></html>\n");
//        }catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
//                summaryPath
//        );
//    }

    public static void createOutPut(int expected, String worker2managerURL, String localReturnQueueUrl, SqsClient sqs, S3Client s3, String bucket) {
        int received = 0;
        List<String> results = new ArrayList<>();
        while (received < expected) {
            ReceiveMessageResponse res =
                    sqs.receiveMessage(ReceiveMessageRequest.builder()
                            .queueUrl(worker2managerURL)
                            .maxNumberOfMessages(1)
                            .waitTimeSeconds(10)
                            .build());

            if (res.hasMessages()) {
                Message m = res.messages().get(0);
                results.add(m.body());
                received++;

                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(worker2managerURL)
                        .receiptHandle(m.receiptHandle())
                        .build());
            }
        }

//        while (received < expected) {
//            ReceiveMessageResponse res =
//                    sqs.receiveMessage(ReceiveMessageRequest.builder()
//                            .queueUrl(worker2managerURL)
//                            .maxNumberOfMessages(1)
//                            .waitTimeSeconds(10)
//                            .build());
//
//            if (res.hasMessages()) {
//                Message m = res.messages().get(0);
//                results.add(m.body());
//                received++;
//
//                sqs.deleteMessage(DeleteMessageRequest.builder()
//                        .queueUrl(worker2managerURL)
//                        .receiptHandle(m.receiptHandle())
//                        .build());
//            }
//            results.add(res.messages().get(0).body());
//            received++;
//            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(worker2managerURL).receiptHandle(res.messages().get(0).receiptHandle()).build());
//        }

        // Build summary HTML
        Path summaryPath = Paths.get("summary.html");
        try (BufferedWriter w = Files.newBufferedWriter(summaryPath)) {
            w.write("<html><body>\n");
            for (String line : results) {
                if (line.contains(" ERROR ")){
                    String[] errParts = line.split(" ERROR ",2);
                    String original = errParts[0];
                    String errType = errParts[1];
                    String[] originalParts = original.split("\\t", 2);
                    String type = originalParts[0];
                    String inputUrl = originalParts[1];
                    w.write(type + ": <a href=\"" + inputUrl + "\">" + inputUrl +
                            "</a> " + errType + "<br/>\n");
                    continue;
                }

                String[] parts = line.split(" ");
                String type = parts[0];
                String inputUrl = parts[1];
                String outBucket = parts[2];
                String outKey = parts[3];

                w.write(type + ": <a href=\"" + inputUrl + "\">" + inputUrl +
                        "</a> -> <a href=\"https://" + outBucket + ".s3.amazonaws.com/" +
                        outKey + "\">result</a><br/>\n");
            }

            w.write("</body></html>");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String summaryKey = "summary-" + System.currentTimeMillis() + ".html";
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(summaryKey).build(),
                summaryPath);

        // Notify the LocalApp
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(localReturnQueueUrl)
                .messageBody(bucket + " " + summaryKey)
                .build());
    }

    public static void main(String[] args) {
        System.out.println("DEBUG: Manager started");
        boolean termianteReq = false;
        Region region = Region.US_EAST_1;
        Ec2Client ec2 = Ec2Client.builder().region(region).build(); // in order to talk to the EC2 services we need EC@ client here as well
        SqsClient sqs = SqsClient.builder().region(region).build();
        S3Client s3 = S3Client.builder().region(region).build();

        GetQueueUrlResponse q = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("LocalToManagerQueue").build()); // the queue name was created on the localApp side and we recieve its address.
        String requestQueueUrl = q.queueUrl();
        CreateQueueRequest queueReqMTW = CreateQueueRequest.builder().queueName("ManagerToWorker").build();
        CreateQueueResponse queueMTW = sqs.createQueue(queueReqMTW);// creating the queue
        String manager2workerURL = queueMTW.queueUrl(); // unique URL to indetify the queue.

        CreateQueueRequest queueReqWTM = CreateQueueRequest.builder().queueName("WorkerToManager").build();
        CreateQueueResponse queueWTM = sqs.createQueue(queueReqWTM);// creating the queue
        String Worker2ManagerURL = queueWTM.queueUrl(); // unique URL to indetify the queue.

        while(!termianteReq){
            System.out.println("DEBUG: In manager loop");
            ReceiveMessageResponse res = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(requestQueueUrl).maxNumberOfMessages(1) //The receipt handle is like a delete token for an SQS message.
                    .waitTimeSeconds(10).build()
            ); // creating the request how to pull from the queue
            if(!res.hasMessages()){
                continue;
            }
            String bodyOfM = res.messages().get(0).body();
            String[] parts = bodyOfM.split(" ");
            String command = parts[0];
            if (command.equals("NEW_TASK")) {
                String bucket = parts[1];
                String fileName = parts[2];
                int limitOfWorkers = Integer.parseInt(parts[3]);
                String localReturnQueueUrl = parts[4];
                // download the input file from S3
                Path localInputPath = Paths.get("managerInput.txt");
                s3.getObject(GetObjectRequest.builder().bucket(bucket).key(fileName).build(), localInputPath);
                // delete message from LTM queue
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(requestQueueUrl).receiptHandle(res.messages().get(0).receiptHandle()).build());
                // send tasks to workers
                int taskCount = fileParser("managerInput.txt", manager2workerURL, sqs);
                // scale workers
                int workersAtMoment = countWorkers(ec2);
                int missingWorkers = (taskCount + limitOfWorkers - 1) / limitOfWorkers;
                int toLaunch = Math.max(0, missingWorkers - workersAtMoment);
                toLaunch = Math.min(toLaunch, 16 - workersAtMoment);
                createWorkers(ec2, toLaunch);
                //createOutPut(taskCount, Worker2ManagerURL, sqs, s3, bucket, "summary.html");
                createOutPut(taskCount, Worker2ManagerURL, localReturnQueueUrl, sqs, s3, bucket);

            } else if (command.equals("TERMINATE")) {
                termianteReq = true;
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(requestQueueUrl).receiptHandle(res.messages().get(0).receiptHandle()).build());
                Filter nameFilter = Filter.builder().name("tag:Name").values("Worker").build(); // find workers
                Filter stateFilter = Filter.builder().name("instance-state-name").values("pending", "running", "stopping", "stopped").build();
                DescribeInstancesResponse resp = ec2.describeInstances(DescribeInstancesRequest.builder().filters(nameFilter, stateFilter).build()); // get the instances
                java.util.List<String> ids = new java.util.ArrayList<>();
                for (Reservation r : resp.reservations()) { // get instances ids
                    for (Instance inst : r.instances()) {
                        ids.add(inst.instanceId());
                    }
                }
                if (!ids.isEmpty()) { // terminate instances
                    ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(ids).build());
                }
            }
        }
    }
}
//response
// └── reservations[]
//       └── instances[] so each reservation can have more then one instance
