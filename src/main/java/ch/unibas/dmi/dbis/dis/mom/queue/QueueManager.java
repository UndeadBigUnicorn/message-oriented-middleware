package ch.unibas.dmi.dbis.dis.mom.queue;

import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Collection of methods for SQS queue management.
 */
public class QueueManager {
    /**
     * Name of the SQS data queue.
     */
    public static final String QUEUE_NAME = "DataQueue";

    /**
     * Returns the SQS data queue URL if it exists, otherwise creates the queue.
     *
     * @return SQS queue URL
     */
    public static String getDataQueue(SqsClient sqs) {
        // TODO: Implement
        return null;
    }

    /**
     * Creates a new SQS queue with the given name.
     *
     * @return SQS queue URL
     */
    public static String createQueue(SqsClient sqs, String queueName) {
        // TODO: Implement
        return null;
    }

    /**
     * Deletes the SQS queue with the given URL.
     */
    public static void deleteQueue(SqsClient sqs, String queueUrl) {
        // TODO: Implement
    }
}
