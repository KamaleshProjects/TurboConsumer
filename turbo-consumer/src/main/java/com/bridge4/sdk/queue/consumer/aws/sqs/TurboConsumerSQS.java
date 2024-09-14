package com.bridge4.sdk.queue.consumer.aws.sqs;

import com.bridge4.sdk.queue.consumer.api.TurboConsumer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TurboConsumerSQS<T> implements TurboConsumer<T> {

    private final String queueUrl;
    private final AtomicBoolean bufferBelowCapacity = new AtomicBoolean(true);
    private final int MAX_INTERNAL_BUFFER_CAPACITY;
    private final BlockingQueue<Message> heapMessageQueue;

    public TurboConsumerSQS(String queueUrl, int maxBufferCapacity) {
        this.queueUrl = queueUrl;
        this.MAX_INTERNAL_BUFFER_CAPACITY = Math.max(INITIAL_CAPACITY, maxBufferCapacity);
        this.heapMessageQueue = new ArrayBlockingQueue<>(this.MAX_INTERNAL_BUFFER_CAPACITY);
    }

    private static final int INITIAL_CAPACITY = 10;
    private static final int RECEIVE_MESSAGE_WAIT_TIME_SECONDS = 0;
    public static final int MAX_NO_OF_MESSAGES = 10;
    private static final String ALL_MESSAGE_ATTRIBUTES = "All";
    private static final SqsClient sqsClient = SqsClient.builder()
            .region(Region.AP_SOUTH_1)
            .build();

    @SuppressWarnings("unchecked")
    @Override
    public Optional<T> poll() {
        int currentCapacity = this.heapMessageQueue.size();
        if (currentCapacity == 0) return Optional.empty();
        Message message = this.heapMessageQueue.poll();
        if (currentCapacity < this.MAX_INTERNAL_BUFFER_CAPACITY) {
            synchronized (this) { this.notify(); }
        }
        return Optional.of((T) message);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> poll(int maxNoOfMessages) {
        if (maxNoOfMessages <= 0) throw new RuntimeException("maxNoOfMessages must be greater than 0");

        int currentCapacity = this.heapMessageQueue.size();
        if (currentCapacity == 0) return new ArrayList<>();
        List<T> messageList = new ArrayList<>();
        int count = 0;
        while (count < maxNoOfMessages) {
            messageList.add((T) this.heapMessageQueue.poll());
            count++;
        }
        return messageList;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void eternalFillCapacity() {
        this.fillCapacity(true);
        while (true) {
            this.fillCapacity(false);
        }
    }

    public void fillCapacity(boolean isInitial) {
        try {
            int fillCount = 0;
            while (!this.bufferBelowCapacity.get() && !isInitial) {
                synchronized (this) {
                    this.wait();
                }
            }
            int heapMessageQueueSize = this.heapMessageQueue.size();
            fillCount = this.MAX_INTERNAL_BUFFER_CAPACITY - heapMessageQueueSize;

            int count = 0;
            while (count < fillCount && count < heapMessageQueueSize - MAX_NO_OF_MESSAGES) {
                List<Message> messageList = receiveMessages(this.queueUrl);
                for (Message message : messageList) {
                    this.heapMessageQueue.add(message);
                    this.deleteMessage(this.queueUrl, message);
                }
                count += messageList.size();
            }

            this.bufferBelowCapacity.set(false);

        } catch (InterruptedException e) {
            // TODO: handle exception
        }
    }

    /**
     * Receives messages from the specified queue (max 10 messages at once).
     *
     * @param queueUrl The URL of the source queue.
     * @return A list of received messages from the queue.
     */
    public List<Message> receiveMessages(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(RECEIVE_MESSAGE_WAIT_TIME_SECONDS)
                .maxNumberOfMessages(MAX_NO_OF_MESSAGES)
                .messageAttributeNames(ALL_MESSAGE_ATTRIBUTES)
                .build();
        return sqsClient.receiveMessage(receiveMessageRequest).messages();
    }

    /**
     * Deletes a message from the specified queue.
     *
     * @param queueUrl The URL of the queue from which the message should be deleted.
     * @param message  The message to be deleted.
     */
    public void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    @Override
    public void startConsumer() {
        Thread fillBufferThread = new Thread(
                this::eternalFillCapacity
        );
        fillBufferThread.start();
    }
}
