package ch.unibas.dmi.dbis.dis.mom.data;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Interface for simple data containers easily transmittable as string.
 * <p>
 * Note: This is a simplified toy example, please do not use this as a reference for good network protocol design.
 */
public interface DataContainer {
    String DELIMITER = ":";

    /**
     * Parses a DataContainer message string into a new DataContainer object.
     *
     * @param data DataContainer message string.
     * @return DataContainer converted from message string.
     */
    static DataContainer parseMessageString(String data) {
        String type = data.split(DELIMITER)[0];

        return switch (type) {
            case TemperatureData.TYPE -> TemperatureData.fromMessageString(data);
            case PressureData.TYPE -> PressureData.fromMessageString(data);
            default -> null;
        };
    }

    /**
     * Converts this DataContainer to a parsable message string.
     *
     * @return Message string containing all information from this DataContainer.
     */
    String toMessageString();

    /**
     * @return Type string of this DataContainer.
     */
    String getType();

    /**
     * Formats the given time in milliseconds into a human-readable string.
     *
     * @param time Time in milliseconds.
     * @return Time as human-readable string.
     */
    static String formatTime(long time) {
        Date timeDate = new Date(time);
        String datePattern = "[yyyy-mm-dd HH-MM-SS]";
        SimpleDateFormat formatter = new SimpleDateFormat(datePattern);
        return formatter.format(timeDate);
    }
}
