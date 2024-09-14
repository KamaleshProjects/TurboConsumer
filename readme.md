# TurboConsumer - High Throughput Messaging Queue Consumer

TurboConsumer provides a high-performance messaging queue consumer interface, designed to mitigate the performance impacts of I/O operations such as polling and message deletion. This project includes a concrete implementation for AWS SQS and demonstrates how to efficiently handle message consumption.

## Key Features

- **High Throughput**: Internal buffering and separate threads optimize message consumption.
- **Flexible API**: The `TurboConsumer` interface is adaptable to various message queue providers.
- **AWS SQS Integration**: A specific implementation for AWS SQS is provided to handle high-throughput scenarios.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [AWS SQS Consumer Example](#aws-sqs-consumer-example)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repository/turbo-consumer.git

2. Build the project using maven:
   ```bash
   mvn clean install

## Usage

### TurboConsumer API

The `TurboConsumer<T>` interface provides the following methods:

### `Optional<T> poll()` 

Retrieves a single message from the queue, if available.

### `List<T> poll(int maxNoOfMessages)`

Retrieves up to `maxNoOfMessages` messages from the queue.

**Parameters:**

- `maxNoOfMessages` (int): The maximum number of messages to retrieve from the queue. Must be greater than 0.

**Returns:**

- `List<T>`: A list containing the messages retrieved from the queue. The list will contain up to `maxNoOfMessages` messages, or fewer if there are not enough messages available.

## AWS SQS Consumer Example

**Example Usage:**

```java
import java.util.List;
import java.util.Optional;

public class TurboConsumerExample {

    public static void main(String[] args) {
        // Assume TurboConsumer is implemented and instantiated
        TurboConsumer<Message> turboConsumer = new TurboConsumerSQS("your-queue-url", 100, Region.YOUR_REGION);

        // Start the consumer
        turboConsumer.startConsumer();

        // Retrieve up to 50 messages from the queue
        List<Message> messages = turboConsumer.poll(50);

        // Process each message
        for (Message message : messages) {
            System.out.println("Received message: " + message.body());
        }
    }
}
   
