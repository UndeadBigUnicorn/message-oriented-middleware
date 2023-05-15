package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.PressureData;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of feature processor subscribing to multiple topics.
 *
 * <p>Calculates and prints rolling averages for temperature and pressure, regardless of associated
 * location.
 */
public class WeatherForecastAggregator extends FeatureProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(WeatherForecastAggregator.class);
  private float rollingAverageTemp;
  private float rollingAveragePressure;

  public static void main(String[] args) {
    runProcessor(new WeatherForecastAggregator());
  }

  @Override
  protected String[] getTopics() {
    return new String[] {TemperatureData.TYPE, PressureData.TYPE};
  }

  @Override
  protected boolean processData(DataContainer data) {
    if (data instanceof TemperatureData temperature) {
      rollingAverageTemp = (rollingAverageTemp + temperature.temperature) / 2;
    } else if (data instanceof PressureData pressure) {
      rollingAveragePressure = (rollingAveragePressure + pressure.pressure) / 2;
    }
    LOG.info(
        "Rolling average temperature: "
            + rollingAverageTemp
            + " °C, rolling average pressure: "
            + rollingAveragePressure
            + " bar.");
    return true;
  }
}
