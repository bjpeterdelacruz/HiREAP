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
    int result = 0;
    if (this.field.equals("source")) {
      result = datum1.getSource().compareTo(datum2.getSource());
    }
    else if (this.field.equals("mtuID")) {
      result =
          datum1.getProperties().getProperty("mtuID")
              .compareTo(datum2.getProperties().getProperty("mtuID"));
    }
    return result;
  }

}
