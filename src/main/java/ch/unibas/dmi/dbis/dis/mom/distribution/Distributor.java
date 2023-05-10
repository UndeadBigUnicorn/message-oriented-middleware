package ch.unibas.dmi.dbis.dis.mom.distribution;

import ch.unibas.dmi.dbis.dis.mom.aws.ClientProvider;
import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueManager;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;

import java.util.Scanner;

/**
 * Middleware component responsible for collecting probe data and redistributing through publish-subscribe.
 */
public class Distributor implements Runnable {
    private final String queueUrl;
    private final AmazonSQS sqsClient;
    private final AmazonSNS snsClient;

    public boolean running = true;

    public Distributor() {
        sqsClient = ClientProvider.getSQSClient();
        queueUrl = QueueManager.getDataQueue(sqsClient);
        snsClient = ClientProvider.getSNSClient();
    }

    /**
     * Main run loop repeatedly calling the <code>receiveMessages()</code> method to poll the data queue.
     */
    @Override
    public void run() {
        try {
            while (running) {
                // Feel free to adjust the polling frequency through this sleep.
                // Beware that polling the queue may count towards the 1 million free SQS requests.
                Thread.sleep(250);
                receiveMessages();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Polls the data queue for new messages, parses them and calls 'publish' to notify subscribers.
     * Messages should only be removed from the data queue once they have been successfully published (when publish
     * returns true).
     */
    private void receiveMessages() {
        // TODO: Implement
    }

    /**
     * Publishes the data based on its type.
     * <p>
     * Note: Feature processors subscribe to topics based on the type string of the data they want to receive. To make
     * sure that they subscribe to the correct topics, publish to those same topics here, or modify the relevant code in
     * the individual feature processors.
     *
     * @param data Data to be published to relevant topics.
     * @return Whether the data was successfully published.
     */
    private boolean publish(DataContainer data) {
        // TODO: Implement
        // To test the first task of the exercise, you can leave this part as it is.
        System.out.println(data);

        return true;
    }

    /**
     * Run this main method to start a distributor from within your IDE.
     */
    public static void main(String[] args) {
        Distributor distributor = new Distributor();

        Thread thread = new Thread(distributor);
        thread.start();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to exit.");
        scanner.nextLine();
        distributor.running = false;
    }
}
