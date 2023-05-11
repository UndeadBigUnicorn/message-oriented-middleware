package ch.unibas.dmi.dbis.dis.mom;

import ch.unibas.dmi.dbis.dis.mom.consumer.TemperatureProcessor;
import ch.unibas.dmi.dbis.dis.mom.consumer.WeatherForecastAggregator;
import ch.unibas.dmi.dbis.dis.mom.distribution.Distributor;
import ch.unibas.dmi.dbis.dis.mom.producer.DeepSeaProbe;
import ch.unibas.dmi.dbis.dis.mom.producer.WeatherBalloon;

/**
 * Main class to simplify running the different components of the system from the JAR generated with the 'shadowJar'
 * gradle task.
 */
public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("No entrypoint provided!");
            System.exit(1);
        }

        String entrypoint = args[0];

        switch (entrypoint) {
            case "Distributor" -> Distributor.main(args);
            case "DeepSeaProbe" -> DeepSeaProbe.main(args);
            case "WeatherBalloon" -> WeatherBalloon.main(args);
            case "TemperatureProcessor" -> TemperatureProcessor.main(args);
            case "WeatherForecastAggregator" -> WeatherForecastAggregator.main(args);
            default -> {
                System.err.println("Unknown entrypoint: " + entrypoint);
                System.exit(1);
            }
        }
    }
}
