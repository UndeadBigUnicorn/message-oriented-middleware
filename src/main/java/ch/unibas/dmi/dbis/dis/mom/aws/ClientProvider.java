package ch.unibas.dmi.dbis.dis.mom.aws;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;

/**
 * Helper class containing functions to instantiate clients for different AWS services.
 */
public class ClientProvider {
    /**
     * @return Amazon SQS client to interact with AWS SQS queues.
     */
    public static AmazonSQS getSQSClient() {
        // TODO: Implement
        return null;
    }

    /**
     * @return Amazon SNS client to interact with AWS SNS topics.
     */
    public static AmazonSNS getSNSClient() {
        // TODO: Implement
        return null;
    }
}
