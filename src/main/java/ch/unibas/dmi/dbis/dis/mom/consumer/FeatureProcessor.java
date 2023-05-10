package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.aws.ClientProvider;
import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.pubsub.PublishSubscribeManager;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueManager;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;

import java.util.Scanner;

/**
 * Abstract class containing shared functions of all feature processors (data consumers).
 * <p>
 * Note that the sqsClient is only required if you use SQS queues as SNS subscription targets.
 */
public abstract class FeatureProcessor implements Runnable {
    private final AmazonSQS sqsClient;
    private final String queueUrl;
    private final AmazonSNS snsClient;
    private final String[] subscriptionArns;

    public boolean running = true;

    /**
     * Base constructor for all feature processors, subscribes this feature processor to the topics defined through the
     * abstract getTopics method.
     */
    public FeatureProcessor() {
        sqsClient = ClientProvider.getSQSClient();
        String queueName = "FeatureProcessor-" + System.currentTimeMillis();
        queueUrl = QueueManager.createQueue(sqsClient, queueName);
        snsClient = ClientProvider.getSNSClient();
        subscriptionArns = PublishSubscribeManager.subscribeToTopics(snsClient, sqsClient, getTopics(), queueUrl);
    }

    /**
     * Main run loop of feature processor
     */
    @Override
    public void run() {
        try {
            while (running) {
                Thread.sleep(250);
                receiveMessages();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // TODO: Implement cleanup
    }

    /**
     * Function called in the main run loop to retrieve and process messages received through the subscribed topics.
     * Once retrieved and parsed, any data container should be passed to the processData method.
     * <p>
     * Note: In case you are not using SQS queues as subscription targets, you may not need this method.
     */
    private void receiveMessages() {
        // TODO: Implement
    }

    /**
     * @return The list of the topics this feature processor wants to subscribe to.
     */
    protected abstract String[] getTopics();

    /**
     * Processes the data received through the subscription
     *
     * @return If the given data was processed successfully.
     */
    protected abstract boolean processData(DataContainer data);

    /**
     * Convenience method for all feature processors to call in their main method to reduce code duplication.
     */
    protected static void runProcessor(FeatureProcessor processor) {
        Thread thread = new Thread(processor);
        thread.start();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to exit.");
        scanner.nextLine();
        processor.running = false;
    }
}
