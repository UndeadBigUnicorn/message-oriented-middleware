package ch.unibas.dmi.dbis.dis.mom.consumer;

import ch.unibas.dmi.dbis.dis.mom.data.DataContainer;
import ch.unibas.dmi.dbis.dis.mom.data.TemperatureData;

/**
 * Minimal example of a feature processor, only prints received temperatures.
 */
public class TemperatureProcessor extends FeatureProcessor {
    @Override
    protected String[] getTopics() {
        return new String[]{TemperatureData.TYPE};
    }

    @Override
    protected boolean processData(DataContainer data) {
        System.out.println(data);
        return true;
    }

    public static void main(String[] args) {
        runProcessor(new TemperatureProcessor());
    }
}
