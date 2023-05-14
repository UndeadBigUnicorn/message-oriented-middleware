package ch.unibas.dmi.dbis.dis.mom.aws;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.regions.Region;

/**
 * Helper class containing functions to instantiate clients for different AWS services.
 */
public final class ClientProvider {

    // hide public constructor
    private ClientProvider() {}

    /*
     * Singletons:
     * Reuse clients since each instance instance of client
     * uses own HTTP connection pool.
     */
    // message queue
    private static SqsClient sqsClient;
    // pub/sub
    private static SnsClient snsClient;

    private static final Region region = Region.US_EAST_1;
    /**
     * @return Amazon SQS client to interact with AWS SQS queues.
     */
    public static SqsClient getSQSClient() {
        if (sqsClient != null) {
            return sqsClient;
        }
        sqsClient = SqsClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        return sqsClient;
    }

    /**
     * @return Amazon SNS client to interact with AWS SNS topics.
     */
    public static SnsClient getSNSClient() {
        if (snsClient != null) {
            return snsClient;
        }
        snsClient = SnsClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        return snsClient;
    }
}
