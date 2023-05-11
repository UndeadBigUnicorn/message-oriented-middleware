package ch.unibas.dmi.dbis.dis.mom.producer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Scanner;

/**
 * Very basic data probe collecting temperature data.
 */
public class DeepSeaProbe extends DataProbe {
    private final Random random = new Random();

    private static final Logger LOG = LoggerFactory.getLogger(DeepSeaProbe.class);

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

    public static void main(String[] args) {
        LOG.info("Greetings from deep sea bottom!");
        DeepSeaProbe probe = new DeepSeaProbe();

        Thread thread = new Thread(probe);
        thread.start();

        Scanner scanner = new Scanner(System.in);
        LOG.info("Press enter to exit.");
        scanner.nextLine();
        probe.running = false;
    }
}
