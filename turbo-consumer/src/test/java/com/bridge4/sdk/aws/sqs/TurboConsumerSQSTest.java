package com.bridge4.sdk.aws.sqs;

import com.bridge4.sdk.queue.consumer.api.TurboConsumer;
import com.bridge4.sdk.queue.consumer.aws.sqs.TurboConsumerSQS;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TurboConsumerSQSTest extends TestCase {

    public TurboConsumerSQSTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TurboConsumerSQSTest.class);
    }

    public void testConsumptionRate() {
        try (SqsClient sqsClient = SqsClient.builder().region(Region.AP_SOUTH_1).build()) {
            String queueUrl = createQueueIfNotExists("qa_turbo_consumer_q", sqsClient);

            for (int i = 0; i < 1000; i++) {
                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(String.valueOf(i))
                        .messageAttributes(new HashMap<>())
                        .build();
                sqsClient.sendMessage(sendMsgRequest);
            }

            TurboConsumer turboConsumer = new TurboConsumerSQS(queueUrl, 100);
            turboConsumer.startConsumer();

            Thread.sleep(1000);

            long start1 = System.nanoTime();
            while (true) {
                List<String> messageList = turboConsumer.poll(100);
                if (messageList.isEmpty()) break;

                for (String message: messageList) {
                    System.out.println(message);
                }
            }
            long end1 = System.nanoTime();

            for (int i = 0; i < 1000; i++) {
                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(String.valueOf(i))
                        .messageAttributes(new HashMap<>())
                        .build();
                sqsClient.sendMessage(sendMsgRequest);
            }

            long start2 = System.nanoTime();
            while (true) {
                List<Message> messageList = receiveMessages(queueUrl, sqsClient);
                if (messageList.isEmpty()) break;

                for (Message message: messageList) {
                    System.out.println(message.body());
                }
            }
            long end2 = System.nanoTime();

            System.out.println("Turbo consumption of 1000 messages took:: " + (end1 - start1) + " nanoseconds");
            System.out.println("consumption of 1000 messages took:: " + (end2 - start2) + " nanoseconds");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Creates a new queue if it does not already exist, with the specified queue attributes
     * if the queue had already been created once inside lifetime of the SQS object then the queueUrl is cached
     * and returned on subsequent calls to createQueueIfNotExists corresponding to the queueName
     *
     * @param queueName The name of the queue to be created.
     * @return The URL of the created or existing queue. sqsClient.createQueue returns the queue url if queue
     * already existing
     */
    public String createQueueIfNotExists(String queueName, SqsClient sqsClient) {
        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
        queueAttributes
                .put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "0");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();
        CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }

    private static final int RECEIVE_MESSAGE_WAIT_TIME_SECONDS = 0;
    public static final int MAX_NO_OF_MESSAGES = 10;
    private static final String ALL_MESSAGE_ATTRIBUTES = "All";

    /**
     * Receives messages from the specified queue (max 10 messages at once).
     *
     * @param queueUrl The URL of the source queue.
     * @return A list of received messages from the queue.
     */
    public List<Message> receiveMessages(String queueUrl, SqsClient sqsClient) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(RECEIVE_MESSAGE_WAIT_TIME_SECONDS)
                .maxNumberOfMessages(MAX_NO_OF_MESSAGES)
                .messageAttributeNames(ALL_MESSAGE_ATTRIBUTES)
                .build();
        return sqsClient.receiveMessage(receiveMessageRequest).messages();
    }
}
