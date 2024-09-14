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

/**
 * <p> Provides API implementation for a messaging queue consumer with
 * <a href="https://aws.amazon.com/sqs/">AWS SQS</a> as the queue provider </p>
 * <p> This implementation attempts to provide a high throughput consumer implementation by buffering the messages
 * in an internal queue </p>
 * <p> The internal buffer is filled by a separate thread that is started by {@link #startConsumer()} that attempts to
 * keep the queue filled to the {@link #MAX_INTERNAL_BUFFER_CAPACITY} </p>
 */
public class TurboConsumerSQS implements TurboConsumer<software.amazon.awssdk.services.sqs.model.Message> {

    private final String queueUrl; // sqs queue url to communicate with the queue

    // to indicate if the buffer is below capacity
    private final AtomicBoolean bufferBelowCapacity = new AtomicBoolean(true);
    private final int MAX_INTERNAL_BUFFER_CAPACITY; // capacity of the internal buffer
    private final BlockingQueue<Message> heapMessageQueue; // internal queue to buffer the polled messages
    private final SqsClient sqsClient; // sqs client to communicate with the sqs queue

    /**
     * @param queueUrl          sqs queue url to communicate with the queue
     * @param maxBufferCapacity capacity of the internal buffer
     * @param region            aws sqs queue region
     */
    public TurboConsumerSQS(String queueUrl, int maxBufferCapacity, Region region) {
        this.queueUrl = queueUrl;
        this.MAX_INTERNAL_BUFFER_CAPACITY = Math.max(INITIAL_CAPACITY, maxBufferCapacity);
        this.heapMessageQueue = new ArrayBlockingQueue<>(this.MAX_INTERNAL_BUFFER_CAPACITY);
        sqsClient = SqsClient.builder().region(region).build();
    }

    private static final int INITIAL_CAPACITY = 10;
    private static final int RECEIVE_MESSAGE_WAIT_TIME_SECONDS = 0;
    public static final int MAX_NO_OF_MESSAGES = 10;
    private static final String ALL_MESSAGE_ATTRIBUTES = "All";

    /**
     * Polls a message from the internal buffer if available.
     * <p>If the buffer size drops below capacity, it triggers a notification to fill the buffer.</p>
     *
     * @return An {@link Optional} containing a message if available, or an empty Optional if the buffer is empty.
     */
    @Override
    public Optional<Message> poll() {
        int currentCapacity = this.heapMessageQueue.size();
        if (currentCapacity == 0) return Optional.empty();
        Message message = this.heapMessageQueue.poll();
        if (currentCapacity < this.MAX_INTERNAL_BUFFER_CAPACITY) {
            synchronized (this) {
                this.notify();
            }
        }
        return Optional.of(message);
    }

    /**
     * Polls up to the specified maximum number of messages from the internal buffer.
     *
     * @param maxNoOfMessages Maximum number of messages to be polled from the buffer.
     * @return A list of messages from the buffer, up to the specified maximum number.
     * @throws RuntimeException if {@code maxNoOfMessages} is less than or equal to zero.
     */
    @Override
    public List<Message> poll(int maxNoOfMessages) {
        if (maxNoOfMessages <= 0) throw new RuntimeException("maxNoOfMessages must be greater than 0");

        int currentCapacity = this.heapMessageQueue.size();
        if (currentCapacity == 0) return new ArrayList<>();
        List<Message> messageList = new ArrayList<>();
        int count = 0;
        while (count < maxNoOfMessages) {
            messageList.add(this.heapMessageQueue.poll());
            count++;
        }
        return messageList;
    }

    /**
     * Continuously fills the internal buffer by polling messages from the SQS queue.
     * <p>This method is designed to run indefinitely to keep the buffer filled.</p>
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public void eternalFillCapacity() {
        this.fillCapacity(true);
        while (true) {
            this.fillCapacity(false);
        }
    }

    /**
     * Fills the internal buffer with messages from the SQS queue, up to the buffer's capacity.
     *
     * @param isInitial Indicates whether this is the initial fill or a refill after the buffer is below capacity.
     */
    public void fillCapacity(boolean isInitial) {
        try {
            int fillCount;
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

    /**
     * Starts the consumer by launching a separate thread to continually fill the internal buffer.
     */
    @Override
    public void startConsumer() {
        Thread fillBufferThread = new Thread(
                this::eternalFillCapacity
        );
        fillBufferThread.start();
    }
}
