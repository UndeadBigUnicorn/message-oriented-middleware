package ch.unibas.dmi.dbis.dis.mom.queue;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

/** Collection of methods for SQS queue management. */
public class QueueManager {
  /** Name of the SQS data queue. */
  public static final String QUEUE_NAME = "DataProbes";

  /**
   * Returns the SQS data queue URL if it exists, otherwise creates the queue.
   *
   * @return SQS queue URL
   */
  public static String getDataQueue(final SqsClient sqsClient) {
    try {
      return getDataQueueUrl(sqsClient, QUEUE_NAME);
    } catch (QueueDoesNotExistException e) {
      return createQueue(sqsClient, QUEUE_NAME);
    }
  }

  /**
   * Creates a new SQS queue with the given name.
   *
   * @return SQS queue URL
   */
  public static String createQueue(final SqsClient sqsClient, String queueName) {
    try {
      sqsClient.createQueue(request -> request.queueName(queueName));
      //                  .attributes(
      //                      Map.of(
      //                          QueueAttributeName.FIFO_QUEUE, "true",
      //                          QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true")));
      return getDataQueueUrl(sqsClient, queueName);
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
    return "";
  }

  /**
   * Retrieve a data queue url.
   *
   * @return SQS queue URL
   */
  private static String getDataQueueUrl(final SqsClient sqsClient, String queueName) {
    return sqsClient.getQueueUrl(request -> request.queueName(queueName)).queueUrl();
  }

  /** Deletes the SQS queue with the given URL. */
  public static void deleteQueue(SqsClient sqsClient, String queueUrl) {
    try {
      sqsClient.deleteQueue(request -> request.queueUrl(queueUrl));
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  /** Send messages into the queue. */
  public static void sendMessage(
      final SqsClient sqsClient,
      final String queueUrl,
      final String message,
      final String messageGroup) {
    try {
      String id = UUID.randomUUID().toString();
      sqsClient.sendMessageBatch(
          request ->
              request
                  .queueUrl(queueUrl)
                  .entries(
                      SendMessageBatchRequestEntry.builder()
                          .id(id)
                          .messageBody(message)
                          //                          .messageGroupId(messageGroup)
                          //                          .messageDeduplicationId(id)
                          .build()));
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  /** Receive messages from the queue. */
  public static List<Message> receiveMessages(final SqsClient sqsClient, final String queueUrl) {
    try {
      return sqsClient.receiveMessage(request -> request.queueUrl(queueUrl)).messages();
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
    return Collections.emptyList();
  }

  /** Delete the message from the queue. */
  public static void deleteMessage(
      final SqsClient sqsClient, final String queueUrl, final Message message) {
    try {
      sqsClient.deleteMessage(
          request -> request.queueUrl(queueUrl).receiptHandle(message.receiptHandle()));
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }
}
