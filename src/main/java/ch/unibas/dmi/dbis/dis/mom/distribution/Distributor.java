package ch.unibas.dmi.dbis.dis.mom.distribution;

import ch.unibas.dmi.dbis.dis.mom.aws.ClientProvider;
import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.pubsub.PublishSubscribeManager;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueManager;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Middleware component responsible for collecting probe data and redistributing through
 * publish-subscribe.
 */
public class Distributor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Distributor.class);
  private final String queueUrl;
  private final SqsClient sqsClient;
  private final SnsClient snsClient;

  public boolean running = true;

  public Distributor() {
    sqsClient = ClientProvider.getSQSClient();
    queueUrl = QueueManager.getDataQueue(sqsClient);
    snsClient = ClientProvider.getSNSClient();
  }

  /** Run this main method to start a distributor from within your IDE. */
  public static void main(String[] args) {
    LOG.info(
        "Welcome from the distributor!\nI'm waiting for messages and I'm ready to dispatch them!");
    Distributor distributor = new Distributor();

    Thread thread = new Thread(distributor);
    thread.start();

    LOG.info("Press enter to exit.");
    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    distributor.running = false;
  }

  /**
   * Main run loop repeatedly calling the <code>receiveMessages()</code> method to poll the data
   * queue.
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
    // close clients at the end
    sqsClient.close();
    snsClient.close();
  }

  /**
   * Polls the data queue for new messages, parses them and calls 'publish' to notify subscribers.
   * Messages should only be removed from the data queue once they have been successfully published
   * (when publish returns true).
   */
  private void receiveMessages() {
    QueueManager.receiveMessages(sqsClient, queueUrl)
        .forEach(
            message -> {
              DataContainer data = DataContainer.parseMessageString(message.body());
              // check if the `publish` was successful
              // and delete the message from the queue afterward
              if (publish(data)) {
                LOG.debug("Deleting the message: " + message.messageId());
                QueueManager.deleteMessage(sqsClient, queueUrl, message);
              }
            });
  }

  /**
   * Publishes the data based on its type.
   *
   * <p>Note: Feature processors subscribe to topics based on the type string of the data they want
   * to receive. To make sure that they subscribe to the correct topics, publish to those same
   * topics here, or modify the relevant code in the individual feature processors.
   *
   * @param data Data to be published to relevant topics.
   * @return Whether the data was successfully published.
   */
  private boolean publish(DataContainer data) {
    LOG.info(data.toString());
    String topicArn = PublishSubscribeManager.getTopic(snsClient, data.getType());
    PublishResponse response =
        snsClient.publish(request -> request.topicArn(topicArn).message(data.toMessageString()));
    return response.sdkHttpResponse().isSuccessful();
  }
}
