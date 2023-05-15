package ch.unibas.dmi.dbis.dis.mom.data;

/**
 * Basic pressure data type containing pressure in bar and the location of the measurement.
 */
public class PressureData implements DataContainer {
    public static final String TYPE = "PRESSURE";

    public final float pressure;
    public final String location;
    public final long time;

    public PressureData(float pressure, String location, long time) {
        this.pressure = pressure;
        this.location = location;
        this.time = time;
    }

    public static PressureData fromMessageString(String data) {
        String[] parts = data.split(DELIMITER);
        return new PressureData(Float.parseFloat(parts[1]), parts[2], Long.parseLong(parts[3]));
    }

    @Override
    public String toString() {
        String dateString = DataContainer.formatTime(time);
        return dateString + " Pressure: " + pressure + " bar at location: " + location;
    }

    @Override
    public String toMessageString() {
        return TYPE + DELIMITER + pressure + DELIMITER + location + DELIMITER + time;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
