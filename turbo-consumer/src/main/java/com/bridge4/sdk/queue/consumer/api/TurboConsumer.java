package com.bridge4.sdk.queue.consumer.api;

import java.util.List;
import java.util.Optional;

/**
 * <p> Provides APIs for a messaging queue consumer </p>
 * <p> Implementations must have some kind of solution to alleviate the jitters caused due to I/O
 * (polling the queue provider for available messages and deleting the message once processing is done)
 * in an attempt to provide a high throughput consumer </p>
 *
 * @param <T> the type of message object (depends on the queue provider)
 */
public interface TurboConsumer<T> {

    /**
     * @return a message if available in the queue, else an empty Optional
     */
    Optional<T> poll();

    /**
     * @param maxNoOfMessages maximum no of messages to be polled and returned
     * @return a list of messages polled from the queue with a length of upto maxNoOfMessages
     */
    List<T> poll(int maxNoOfMessages);

    /**
     * starts the consumer, a guaranteed handshake between the client and the {@link TurboConsumer} implementation
     * that this method will be called for starting the consumer before
     * consuming messages via {@link #poll()} or {@link #poll(int maxNoOfMessages)}
     */
    void startConsumer();
}
