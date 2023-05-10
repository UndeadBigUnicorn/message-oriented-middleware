package ch.unibas.dmi.dbis.dis.mom.data;

/**
 * Basic temperature data type containing temperature in degrees Celsius, and the location and time of the measurement.
 */
public class TemperatureData implements DataContainer {
    public static final String TYPE = "TEMPERATURE";

    public final float temperature;
    public final String location;
    public final long time;

    public TemperatureData(float temperature, String location, long time) {
        this.temperature = temperature;
        this.location = location;
        this.time = time;
    }

    @Override
    public String toString() {
        String dateString = DataContainer.formatTime(time);
        return dateString + " Temperature: " + String.format("%.2f", temperature) + " °C at location: " + location;
    }

    @Override
    public String toMessageString() {
        return TYPE + DELIMITER + temperature + DELIMITER + location + DELIMITER + time;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static TemperatureData fromMessageString(String data) {
        String[] parts = data.split(DELIMITER);
        return new TemperatureData(Float.parseFloat(parts[1]), parts[2], Long.parseLong(parts[3]));
    }
}
