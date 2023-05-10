package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.PressureData;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;

/**
 * Example of feature processor subscribing to multiple topics.
 * <p>
 * Calculates and prints rolling averages for temperature and pressure, regardless of associated location.
 */
public class WeatherForecastAggregator extends FeatureProcessor {
    private float rollingAverageTemp;
    private float rollingAveragePressure;

    @Override
    protected String[] getTopics() {
        return new String[]{TemperatureData.TYPE, PressureData.TYPE};
    }

    @Override
    protected boolean processData(DataContainer data) {
        if (data instanceof TemperatureData) {
            TemperatureData temperature = (TemperatureData) data;
            rollingAverageTemp = (rollingAverageTemp + temperature.temperature) / 2;
        } else if (data instanceof PressureData) {
            PressureData pressure = (PressureData) data;
            rollingAveragePressure = (rollingAveragePressure + pressure.pressure) / 2;
        }
        System.out.println("Rolling average temperature: " + rollingAverageTemp + " °C, rolling average pressure: "
                + rollingAveragePressure + " bar.");
        return true;
    }

    public static void main(String[] args) {
        runProcessor(new WeatherForecastAggregator());
    }
}
