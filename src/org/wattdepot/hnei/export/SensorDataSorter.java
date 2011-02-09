package org.wattdepot.hnei.export;

import java.io.Serializable;
import java.util.Comparator;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * This class is used to sort SensorData objects.
 * 
 * @author BJ Peter DeLaCruz
 */
public class SensorDataSorter implements Comparator<SensorData>, Serializable {

  /** Field to sort by, used to compare two SensorData objects. */
  private String field;

  /**
   * Used when serializing instances of this class.
   */
  private static final long serialVersionUID = -1147427828325805371L;

  /**
   * Creates a new SensorDataSorter object.
   * 
   * @param field Field to sort by, used to compare two SensorData objects.
   */
  public SensorDataSorter(String field) {
    this.field = field;
  }

  /**
   * Compares two SensorData objects.
   * 
   * @param datum1 First SensorData object.
   * @param datum2 Second SensorData object.
   * @return -1 if datum1 < datum2, 0 if datum1 == datum2, or 1 if datum1 > datum2.
   */
  @Override
  public int compare(SensorData datum1, SensorData datum2) {
    String property1 = null;
    String property2 = null;
    int result = 0;

    if (this.field.equals("account")) {
      result = datum1.getSource().compareTo(datum2.getSource());
    }
    else if (this.field.equals("mtuID")) {
      property1 = datum1.getProperties().getProperty("mtuID");
      property2 = datum2.getProperties().getProperty("mtuID");
      result = property1.compareTo(property2);
    }
    else if (this.field.equals("reading")) {
      Integer reading1 = new Integer(datum1.getProperties().getProperty("reading"));
      Integer reading2 = new Integer(datum2.getProperties().getProperty("reading"));
      result = reading1.compareTo(reading2);
    }
    else if (this.field.equals("timestamp")) {
      property1 = datum1.getTimestamp().toString();
      property2 = datum2.getTimestamp().toString();
      result = property1.compareTo(property2);
    }
    else if (this.field.equals("isMonotonicallyIncreasing")) {
      property1 = datum1.getProperties().getProperty("isMonotonicallyIncreasing");
      property2 = datum2.getProperties().getProperty("isMonotonicallyIncreasing");
      result = property1.compareTo(property2);
    }
    else if (this.field.equals("hourly")) {
      property1 = datum1.getProperties().getProperty("daily");
      property2 = datum2.getProperties().getProperty("daily");
      if (property1 == null) {
        result = -1;
      }
      else if (property2 == null) {
        result = 1;
      }
      else {
        result = property1.compareTo(property2);
      }
    }
    else if (this.field.equals("daily")) {
      property1 = datum1.getProperties().getProperty("hourly");
      property2 = datum2.getProperties().getProperty("hourly");
      if (property1 == null) {
        result = -1;
      }
      else if (property2 == null) {
        result = 1;
      }
      else {
        result = property1.compareTo(property2);
      }
    }

    return result;
  }

}
