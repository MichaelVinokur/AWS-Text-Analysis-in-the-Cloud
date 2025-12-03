package com.distributed.assignment1;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.TypedDependency;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;


public class Worker {

    private static final Region REGION = Region.US_EAST_1;
    private static final String BUCKET = "mtmv-bguproject";

    //(slow!)
    private static final LexicalizedParser PARSER =
            LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");  // loads that grammar file one time, and builds the parser object

    private static final TokenizerFactory<CoreLabel> TOKENIZER_FACTORY =
            PTBTokenizer.factory(new CoreLabelTokenFactory(), "");  //creates a reusable tokenizer

    public static void main(String[] args) {
        System.out.println("DEBUG: Worker loop iteration. Waiting for messages...");
        S3Client s3 = S3Client.builder().region(REGION).build();
        SqsClient sqs = SqsClient.builder().region(REGION).build();
        String taskQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("ManagerToWorker").build()).queueUrl();
        String resultQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("WorkerToManager").build()).queueUrl();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        while (true) {
            ReceiveMessageResponse resp = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(taskQueueUrl).maxNumberOfMessages(1).waitTimeSeconds(20).visibilityTimeout(30).build());
            for (Message msg : resp.messages()) {
                String body = msg.body();
                System.out.println("DEBUG: Processing message: " + msg.body());
                try {
                    String[] parts = body.split("\\s+", 2);
                    if (parts.length < 2) {
                        System.err.println("DEBUG ERROR: Invalid message format (missing URL): " + body);
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(taskQueueUrl).receiptHandle(msg.receiptHandle()).build());
                        continue;
                    }
                    String analysisType = parts[0];
                    String url = parts[1];
                    Future<String> future = executor.submit(() -> {
                        String text = downloadText(url);
                        System.out.println("DEBUG: Downloaded text. Length: " + text.length());
                        return performAnalysis(text, analysisType);
                    });
                    String analyzed = null;
                    while (analyzed == null) {
                        try {
                            analyzed = future.get(10, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            System.out.println("DEBUG: Extending visibility for " + url);
                            sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                                    .queueUrl(taskQueueUrl)
                                    .receiptHandle(msg.receiptHandle())
                                    .visibilityTimeout(30) // הארכה ב-30 שניות נוספות
                                    .build());
                        }
                    }
                    System.out.println("DEBUG: Analysis complete.");

                    String key = "results/" + UUID.randomUUID() + ".txt";

                    s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromString(analyzed));
                    String resultMsg = analysisType + " " + url + " " + BUCKET + " " + key;
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(resultQueueUrl).messageBody(resultMsg).build());
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(taskQueueUrl).receiptHandle(msg.receiptHandle()).build());
                    System.out.println("DEBUG: Task finished successfully.");
                } catch (Exception e) {
                    System.err.println("DEBUG ERROR: Worker crashed processing message.");
                    String errorMsg = body + " ERROR " + e.getClass().getSimpleName();
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(resultQueueUrl).messageBody(errorMsg).build());
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(taskQueueUrl).receiptHandle(msg.receiptHandle()).build());
                }
            }
        }
    }

    private static String downloadText(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private static String performAnalysis(String text, String analysisType) throws Exception {
    StringBuilder out = new StringBuilder();
    String[] lines = text.split("\\R+");// Split text into lines so we don’t crash on long files
    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty()) continue;
        List<CoreLabel> tokens = TOKENIZER_FACTORY.getTokenizer(new StringReader(line)).tokenize();//TOKENIZE  "Hello world!" → ["Hello", "world", "!"]
        Tree parseTree = PARSER.apply(tokens);//PARSE   tree from the tokens
        if (analysisType.equals("POS")) {// list of tagged words: word + POS tag
            for (TaggedWord tw : parseTree.taggedYield()) {
                out.append(tw.word()).append("_").append(tw.tag()).append(" ");
            }
            out.append("\n");
        } else if (analysisType.equals("CONSTITUENCY")) {
            out.append(parseTree.toString()).append("\n");// whole parse tree (context-free grammar)
        } else if (analysisType.equals("DEPENDENCY")) {// dependency grammar (relations between words)
            PennTreebankLanguagePack tlp = new PennTreebankLanguagePack();
            GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
            GrammaticalStructure gs = gsf.newGrammaticalStructure(parseTree);
            for (TypedDependency td : gs.typedDependenciesCCprocessed()) {
                out.append(td.toString()).append("\n");
            }
            out.append("\n");
        }else {
                throw new IllegalArgumentException("Unknown type: " + analysisType);
            }
        }
        return out.toString();
    }

}

