package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.aws.ClientProvider;
import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.pubsub.PublishSubscribeManager;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueManager;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Map;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Abstract class containing shared functions of all feature processors (data consumers).
 *
 * <p>Note that the sqsClient is only required if you use SQS queues as SNS subscription targets.
 */
public abstract class FeatureProcessor implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FeatureProcessor.class);
  private final SqsClient sqsClient;
  private final String queueUrl;
  private final SnsClient snsClient;
  private final String[] subscriptionArns;

  public boolean running = true;

  /**
   * Base constructor for all feature processors, subscribes this feature processor to the topics
   * defined through the abstract getTopics method.
   */
  public FeatureProcessor() {
    sqsClient = ClientProvider.getSQSClient();
    String queueName = "FeatureProcessor-" + System.currentTimeMillis();
    queueUrl = QueueManager.createQueue(sqsClient, queueName);
    snsClient = ClientProvider.getSNSClient();
    subscriptionArns =
        PublishSubscribeManager.subscribeToTopics(snsClient, sqsClient, getTopics(), queueUrl);
  }

  /**
   * Convenience method for all feature processors to call in their main method to reduce code
   * duplication.
   */
  protected static void runProcessor(FeatureProcessor processor) {
    Thread thread = new Thread(processor);
    thread.start();

    LOG.info("Press enter to exit.");
    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    processor.running = false;
  }

  /** Main run loop of feature processor */
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

    // clean up
    // unsubscribe from topics
    PublishSubscribeManager.unsubscribeFromTopics(snsClient, subscriptionArns);
    // delete queue
    QueueManager.deleteQueue(sqsClient, queueUrl);
    // close clients
    sqsClient.close();
    snsClient.close();
  }

  /**
   * Function called in the main run loop to retrieve and process messages received through the
   * subscribed topics. Once retrieved and parsed, any data container should be passed to the
   * processData method.
   *
   * <p>Note: In case you are not using SQS queues as subscription targets, you may not need this
   * method.
   */
  private void receiveMessages() {
    QueueManager.receiveMessages(sqsClient, queueUrl)
        .forEach(
            message -> {
              // message is json with many fields, only "Message" field is needed
              String messageText = null;
              try {
                // Parse the message body JSON string to extract the actual message text
                TypeToken<Map<String, String>> mapType = new TypeToken<>() {};
                Map<String, String> messageBodyJson = new Gson().fromJson(message.body(), mapType);
                messageText = messageBodyJson.get("Message");
              } catch (Exception e) {
                LOG.error("Failed to extract message text from SQS message: " + e.getMessage());
                return;
              }
              DataContainer data = DataContainer.parseMessageString(messageText);
              // check if the `publish` was successful
              // and delete the message from the queue afterward
              if (processData(data)) {
                LOG.debug("Deleting the message: " + message.messageId());
                QueueManager.deleteMessage(sqsClient, queueUrl, message);
              }
            });
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
}
