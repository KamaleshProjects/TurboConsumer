package com.bridge4.sdk.queue.consumer.api;

import java.util.List;
import java.util.Optional;

/**
 * Provides APIs for a messaging queue consumer
 */
public interface TurboConsumer<T> {

    /**
     *
     * @return
     */
    Optional<T> poll();

    /**
     *
     * @param maxNoOfMessages
     * @return
     */
    List<T> poll(int maxNoOfMessages);

    /**
     *
     */
    void eternalFillCapacity();

    /**
     *
     */
    void startConsumer();
}
