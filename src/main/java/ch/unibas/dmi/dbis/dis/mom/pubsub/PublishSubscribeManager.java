package ch.unibas.dmi.dbis.dis.mom.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/** Collection of methods to manage SNS topics and subscriptions. */
public class PublishSubscribeManager {
  private static final String QUEUE_POLICY_TEMPLATE =
      "{\n"
          + "  \"Version\": \"2008-10-17\",\n"
          + "  \"Id\": \"__default_policy_ID\",\n"
          + "  \"Statement\": [\n"
          + "    {\n"
          + "      \"Effect\": \"Allow\",\n"
          + "      \"Principal\": {\n"
          + "        \"AWS\": \"*\"\n"
          + "      },\n"
          + "      \"Action\": \"sqs:SendMessage\",\n"
          + "      \"Resource\": \"%s\",\n"
          + "      \"Condition\": {\n"
          + "        \"ArnEquals\": {\n"
          + "          \"aws:SourceArn\": [%s]\n"
          + "        }\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}";
  /**
   * Retrieves the topic ARN for a given topic name. If the topic does not yet exist, it is created.
   *
   * @return SNS Topic ARN
   */
  public static String getTopic(SnsClient snsClient, String topicName) {
    try {
      // create createTopic will return the topic it was created already
      return snsClient.createTopic(request -> request.name(topicName)).topicArn();
    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
    return "";
  }

  /**
   * Deletes the topic with the given ARN.
   *
   * <p>Depending on your implementation, you may never need to use this method.
   */
  public static void deleteTopic(SnsClient snsClient, String topicArn) {
    try {
      snsClient.deleteTopic(request -> request.topicArn(topicArn));
    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  /**
   * Subscribes the SQS queue specified by the queue URL to the given SNS topics.
   *
   * <p>Note: If you are using a different SNS subscription method than SQS queues, you will need to
   * modify the signature of this method.
   *
   * @param topics Array of topic names to subscribe to
   * @param queueUrl URL of the SQS queue to subscribe to the topics
   * @return SNS subscription ARNs corresponding to the topic subscriptions
   */
  public static String[] subscribeToTopics(
      SnsClient snsClient, SqsClient sqsClient, String[] topics, String queueUrl) {
    try {
      QueueAttributeName arnQueueAttribute = QueueAttributeName.QUEUE_ARN;
      String sqsQueueArn =
          sqsClient
              .getQueueAttributes(
                  request -> request.queueUrl(queueUrl).attributeNames(arnQueueAttribute))
              .attributes()
              .get(arnQueueAttribute);
      List<String> subscriptions = new ArrayList<>();
      List<String> topicArns = new ArrayList<>();
      for (String topic : topics) {
        String topicArn = getTopic(snsClient, topic);
        topicArns.add("\"" + topicArn + "\"");
        // subscribe to the topic
        subscriptions.add(
            snsClient
                .subscribe(
                    request -> request.topicArn(topicArn).protocol("sqs").endpoint(sqsQueueArn))
                .subscriptionArn());
      }
      // allow topic to send messages to the queue
      String policy =
          String.format(QUEUE_POLICY_TEMPLATE, sqsQueueArn, String.join(",", topicArns));
      sqsClient.setQueueAttributes(
          request ->
              request.queueUrl(queueUrl).attributes(Map.of(QueueAttributeName.POLICY, policy)));
      return subscriptions.toArray(new String[0]);
    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
    return new String[0];
  }

  /**
   * Content based subscription filters.
   *
   * @param subscriptionArns Array of subscription arns to filter
   */
  public static void contentBasedFilterSubscription(
      SnsClient snsClient,
      String[] subscriptionArns,
      final String location,
      final int lowerTemperatureBound,
      final int upperTemperatureBound) {
    try {
      for (String subscriptionArn : subscriptionArns) {
        SNSMessageFilterPolicy fp = new SNSMessageFilterPolicy();
        // Add a filter policy attribute with a single value
        fp.addAttribute("Location", location);
        // Add a numeric attribute with a range
        fp.addAttributeRange(
            "Temperature", ">", lowerTemperatureBound, "<=", upperTemperatureBound);
        // Apply the filter policy attributes to an Amazon SNS subscription
        fp.apply(snsClient, subscriptionArn);
      }
    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }

  /** Deletes the subscriptions corresponding with the given subscription ARNs. */
  public static void unsubscribeFromTopics(SnsClient snsClient, String[] subscriptionArns) {
    try {
      for (String subscriptionArn : subscriptionArns) {
        snsClient.unsubscribe(request -> request.subscriptionArn(subscriptionArn));
      }
    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }
}
