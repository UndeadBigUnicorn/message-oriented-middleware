package ch.unibas.dmi.dbis.dis.mom.producer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;
import java.util.Random;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Very basic data probe collecting temperature data. */
public class DeepSeaProbe extends DataProbe {
  private static final Logger LOG = LoggerFactory.getLogger(DeepSeaProbe.class);
  private final Random random = new Random();

  public static void main(String[] args) {
    LOG.info("Greetings from deep sea bottom!");
    DeepSeaProbe probe = new DeepSeaProbe();

    Thread thread = new Thread(probe);
    thread.start();

    LOG.info("Press enter to exit.");
    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    probe.running = false;
  }

  @Override
  protected DataContainer collectData() {
    float temp = random.nextFloat() * 3 + 0.5f;
    long time = System.currentTimeMillis();
    return new TemperatureData(temp, "Deep Sea", time);
  }

  @Override
  protected int getDataDelay() {
    return random.nextInt(5000) + 100;
  }
}
