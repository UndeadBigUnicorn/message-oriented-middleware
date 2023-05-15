package ch.unibas.dmi.dbis.dis.mom.queue;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.UUID;

/** Collection of methods for SQS queue management. */
public class QueueManager {
  /** Name of the SQS data queue. */
  public static final String QUEUE_NAME = "DataProbes.fifo";

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
      CreateQueueRequest createQueueRequest =
          CreateQueueRequest.builder().queueName(queueName).build();

      sqsClient.createQueue(createQueueRequest);

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
    GetQueueUrlResponse getQueueUrlResponse =
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
    return getQueueUrlResponse.queueUrl();
  }

  /** Deletes the SQS queue with the given URL. */
  public static void deleteQueue(SqsClient sqsClient, String queueUrl) {
    try {
      DeleteQueueRequest deleteQueueRequest =
          DeleteQueueRequest.builder().queueUrl(queueUrl).build();

      sqsClient.deleteQueue(deleteQueueRequest);
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
      SendMessageBatchRequest sendMessageBatchRequest =
          SendMessageBatchRequest.builder()
              .queueUrl(queueUrl)
              .entries(
                  SendMessageBatchRequestEntry.builder()
                      .id(id)
                      .messageBody(message)
                      .messageGroupId(messageGroup)
                      .messageDeduplicationId(id)
                      .build())
              .build();
      sqsClient.sendMessageBatch(sendMessageBatchRequest);
    } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }
}
