package ch.unibas.dmi.dbis.dis.mom.pubsub;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;

/**
 * Collection of methods to manage SNS topics and subscriptions.
 */
public class PublishSubscribeManager {
    /**
     * Retrieves the topic ARN for a given topic name.
     * If the topic does not yet exists, it is created.
     *
     * @return SNS Topic ARN
     */
    public static String getTopic(AmazonSNS snsClient, String topicName) {
        // TODO: Implement
        return null;
    }

    /**
     * Deletes the topic with the given ARN.
     * <p>
     * Depending on your implementation, you may never need to use this method.
     */
    public static void deleteTopic(AmazonSNS snsClient, String topicArn) {
        // TODO: Implement
    }

    /**
     * Subscribes the SQS queue specified by the queue URL to the given SNS topics.
     * <p>
     * Note: If you are using a different SNS subscription method than SQS queues, you will need to modify the signature
     * of this method.
     *
     * @param topics   Array of topic names to subscribe to
     * @param queueUrl URL of the SQS queue to subscribe to the topics
     * @return SNS subscription ARNs corresponding to the topic subscriptions
     */
    public static String[] subscribeToTopics(AmazonSNS snsClient, AmazonSQS sqsClient, String[] topics, String queueUrl) {
        // TODO: Implement
        return null;
    }

    /**
     * Deletes the subscriptions corresponding with the given subscription ARNs.
     */
    public static void unsubscribeFromTopics(AmazonSNS snsClient, String[] subscriptionArns) {
        // TODO: Implement
    }
}
