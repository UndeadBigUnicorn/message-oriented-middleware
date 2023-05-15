package ch.unibas.dmi.dbis.dis.mom.producer;

import ch.unibas.dmi.dbis.dis.mom.aws.ClientProvider;
import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Abstract class containing shared functions of all data probes (data producers). */
public abstract class DataProbe implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DataProbe.class);
  private final SqsClient sqsClient;
  private final String queueUrl;

  public boolean running = true;

  protected DataProbe() {
    sqsClient = ClientProvider.getSQSClient();
    queueUrl = QueueManager.getDataQueue(sqsClient);
  }

  /**
   * Main run loop of data probe. Repeatedly 'collects data' over a period of time, then sends the
   * data to the data queue.
   */
  @Override
  public void run() {
    try {
      while (running) {
        Thread.sleep(getDataDelay());
        DataContainer data = collectData();
        LOG.info("Collected data: " + data);
        sendData(data.toMessageString(), data.getType());
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Sends the data to the data queue.
   *
   * @param message DataContainer message string.
   * @param messageGroup DataContainer type.
   */
  protected void sendData(final String message, final String messageGroup) {
    QueueManager.sendMessage(sqsClient, queueUrl, message, messageGroup);
  }

  /**
   * @return DataContainer containing data 'collected' by the probe.
   */
  protected abstract DataContainer collectData();

  /**
   * @return The amount of time in milliseconds to wait for the probe to collect data.
   */
  protected abstract int getDataDelay();
}
