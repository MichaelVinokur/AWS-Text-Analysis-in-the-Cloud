# Distributed Text Analysis in the Cloud (AWS)

A distributed system in **Java** that runs NLP analysis on large text files using a **Manager–Worker** architecture on **AWS**. A local client submits a task, a Manager node on EC2 splits it into jobs, a pool of Worker nodes processes them in parallel, and the client receives a combined HTML report.

**Tech:** Java 17 · AWS SDK for Java v2 · EC2 · S3 · SQS · Stanford Parser (NLP) · Maven

---

## Architecture

```
Local App ──(S3 upload + SQS msg)──► Manager (EC2) ──(SQS jobs)──► Workers (EC2 × N)
    ▲                                    │                              │
    └──────(SQS: summary ready)──────────┴◄──────(SQS results)──────────┘
                                   HTML summary on S3
```

1. **Local Application** – uploads the input file to S3, starts a Manager on EC2 if none is running, and sends a `NEW_TASK` message over SQS.
2. **Manager** – reads each task, breaks the input into one job per line and sends them to a work queue. It launches Worker instances according to the load (one worker per *n* jobs, up to a limit). It handles several clients at the same time using a thread pool (`ExecutorService`).
3. **Workers** – pull jobs from SQS, download the text and run the requested analysis with the Stanford Parser:
   - `POS` – part-of-speech tagging
   - `CONSTITUENCY` – constituency parsing
   - `DEPENDENCY` – dependency parsing

   Each worker uploads its result to S3 and reports back over SQS.
4. **Manager** – collects all results into a single HTML summary, uploads it to S3 and notifies the client.
5. **Termination** – with the `terminate` flag, the Manager finishes all in-flight tasks and then shuts down the workers and itself.

## Design highlights
- **Scalability:** the number of workers is derived from the workload (`n` = jobs per worker).
- **Concurrency:** the Manager serves several local clients at once through a thread pool.
- **Fault tolerance:** a message is deleted from SQS only after it has been processed. Workers extend the message's visibility timeout during long jobs. If a worker fails mid-job, the job becomes visible again and another worker picks it up.
- **Security:** no credentials are stored in code. EC2 instances use an IAM instance profile.

## Input format
Tab-separated lines of `<ANALYSIS_TYPE>\t<URL of text file>`:
```
POS             https://www.gutenberg.org/files/1659/1659-0.txt
CONSTITUENCY    https://www.gutenberg.org/files/1659/1659-0.txt
DEPENDENCY      https://www.gutenberg.org/files/1659/1659-0.txt
```

## Build & run
```bash
cd Assignment1TextAnalysisintheCloud
mvn clean package
java -jar target/<jar-name>.jar <inputFile> <outputFile.html> <n> [terminate]
```
Prerequisites: AWS credentials configured locally (`~/.aws/credentials` or environment variables), plus an IAM instance profile with S3 and SQS access for the EC2 instances.

---
*Team project – Distributed System Programming course, Ben-Gurion University.*
