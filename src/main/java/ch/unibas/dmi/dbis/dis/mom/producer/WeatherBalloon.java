package ch.unibas.dmi.dbis.dis.mom.producer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.PressureData;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;

import java.util.Random;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very basic data probe collecting temperature and pressure data.
 */
public class WeatherBalloon extends DataProbe {

    private final Random random = new Random();

    private static final Logger LOG = LoggerFactory.getLogger(WeatherBalloon.class);

    @Override
    protected DataContainer collectData() {
        long time = System.currentTimeMillis();
        if (random.nextBoolean()) {
            float temp = (float) random.nextGaussian() * -16 - 32;
            return new TemperatureData(temp, "Stratosphere", time);
        } else {
            float pressure = random.nextFloat() * 0.001f + 0.0001f;
            return new PressureData(pressure, "Stratosphere", time);
        }
    }

    @Override
    protected int getDataDelay() {
        return random.nextInt(2500) + 100;
    }

    public static void main(String[] args) {
        LOG.info("Greetings from weather balloon. \n F-22, please don't shoot me!");
        WeatherBalloon probe = new WeatherBalloon();

        Thread thread = new Thread(probe);
        thread.start();

        LOG.info("Press enter to exit.");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        probe.running = false;
    }
}
