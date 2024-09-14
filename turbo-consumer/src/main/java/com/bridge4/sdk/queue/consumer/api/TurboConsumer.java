package com.bridge4.sdk.queue.consumer.api;

import java.util.List;
import java.util.Optional;

public interface TurboConsumer {

    Optional<String> poll();

    List<String> poll(int maxNoOfMessages);

    void eternalFillCapacity();

    void startConsumer();
}
