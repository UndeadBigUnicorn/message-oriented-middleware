package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Minimal example of a feature processor, only prints received temperatures. */
public class TemperatureProcessor extends FeatureProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TemperatureProcessor.class);

  public static void main(String[] args) {
    runProcessor(new TemperatureProcessor());
  }

  @Override
  protected String[] getTopics() {
    return new String[] {TemperatureData.TYPE};
  }

  @Override
  protected boolean processData(DataContainer data) {
    LOG.info(data.toString());
    return true;
  }
}
