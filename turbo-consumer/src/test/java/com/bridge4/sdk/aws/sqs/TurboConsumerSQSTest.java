package com.bridge4.sdk.aws.sqs;

import com.bridge4.sdk.queue.consumer.api.TurboConsumer;
import com.bridge4.sdk.queue.consumer.aws.sqs.TurboConsumerSQS;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test class for the TurboConsumerSQS.
 */
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

    /**
     * So that maven does not complain about missing tests
     */
    public void testSilent() {

    }

    /**
     * Test case to measure the message consumption rate using TurboConsumerSQS and
     * compare it with the traditional SQS message consumption.
     * <p>This test sends 1000 messages to the SQS queue and then consumes them using the
     * {@link TurboConsumerSQS}. It compares the time taken to consume the messages
     * with that of manually polling messages from SQS.</p>
     */
    public void disabledTestConsumptionRate() {
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

            TurboConsumer<Message> turboConsumer = new TurboConsumerSQS(
                    queueUrl, 100, Region.AP_SOUTH_1
            );
            turboConsumer.startConsumer();

            Thread.sleep(1000);

            long start1 = System.nanoTime();
            while (true) {
                List<Message> messageList = turboConsumer.poll(100);
                if (messageList.isEmpty()) break;

                for (Message message: messageList) {
                    System.out.println(message.body());
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
                    this.deleteMessage(queueUrl, message, sqsClient);
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

    /**
     * Deletes a message from the specified queue.
     *
     * @param queueUrl The URL of the queue from which the message should be deleted.
     * @param message  The message to be deleted.
     */
    public void deleteMessage(String queueUrl, Message message, SqsClient sqsClient) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public void disabledTestPerformanceGraph() throws InterruptedException, IOException {
        SqsClient sqsClient = SqsClient.builder().region(Region.AP_SOUTH_1).build();
        String queueUrl = createQueueIfNotExists("qa_turbo_consumer_q", sqsClient);

        int[] messageCounts = {10, 100, 1000, 10000};
        XYSeries turboConsumerSeries = new XYSeries("TurboConsumer");
        XYSeries normalConsumerSeries = new XYSeries("Normal Consumer");

        // Test with varying message counts
        for (int messageCount : messageCounts) {
            // Add messages to the queue
            sendMessages(queueUrl, sqsClient, messageCount);

            // TurboConsumer performance
            TurboConsumer<Message> turboConsumer = new TurboConsumerSQS(queueUrl, 100, Region.AP_SOUTH_1);
            turboConsumer.startConsumer();
            long turboStartTime = System.nanoTime();
            consumeMessagesWithTurbo(turboConsumer, messageCount);
            long turboEndTime = System.nanoTime();
            turboConsumerSeries.add(messageCount, (turboEndTime - turboStartTime));

            // Add messages to the queue
            sendMessages(queueUrl, sqsClient, messageCount);
            // Normal consumption performance
            long normalStartTime = System.nanoTime();
            consumeMessagesNormally(queueUrl, sqsClient, messageCount);
            long normalEndTime = System.nanoTime();
            normalConsumerSeries.add(messageCount, (normalEndTime - normalStartTime));
        }

        // Plotting the graph
        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(turboConsumerSeries);
        dataset.addSeries(normalConsumerSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "TurboConsumer vs Normal Consumer Performance",
                "Number of Messages",
                "Time Taken (nanoseconds, logarithmic scale)",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // Set Y-axis to logarithmic scale for better visibility
        XYPlot plot = chart.getXYPlot();
        plot.setRangeAxis(new LogarithmicAxis("Time Taken (nanoseconds, logarithmic scale)"));

        // Display the chart
        JFrame chartFrame = new JFrame();
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));
        chartFrame.setContentPane(chartPanel);
        chartFrame.pack();
        chartFrame.setVisible(true);

        File chartFile = new File("/Users/kamaleshs/Documents/personal/TurboConsumerPerformanceTest.png");
        ChartUtils.saveChartAsPNG(chartFile, chart, 800, 600);

    }

    public void sendMessages(String queueUrl, SqsClient sqsClient, int count) {
        for (int i = 0; i < count; i++) {
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("Message " + i)
                    .messageAttributes(new HashMap<>())
                    .build();
            sqsClient.sendMessage(sendMsgRequest);
        }
    }

    public void consumeMessagesWithTurbo(TurboConsumer<Message> turboConsumer, int messageCount) throws InterruptedException {
        List<Message> messages;
        do {
            messages = turboConsumer.poll(messageCount);
        } while (!messages.isEmpty());
    }

    public void consumeMessagesNormally(String queueUrl, SqsClient sqsClient, int messageCount) {
        int received = 0;
        while (received < messageCount) {
            List<Message> messages = receiveMessages(queueUrl, sqsClient);
            received += messages.size();
            for (Message message : messages) {
                deleteMessage(queueUrl, message, sqsClient);
            }
        }
    }
}
