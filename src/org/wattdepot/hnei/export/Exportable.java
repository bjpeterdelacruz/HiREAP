package org.wattdepot.hnei.export;

import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * Classes that implement this interface will generate a CSV file containing data for one or more
 * sources.
 * 
 * @author BJ Peter DeLaCruz
 */
public interface Exportable {

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  public String getTableHeader();

  /**
   * Returns information stored in a SensorData object.
   * 
   * @param datum SensorData object from which to extract information.
   * @return Information stored in the SensorData object.
   */
  public String getInfo(SensorData datum);

  /**
   * Gets sensor data for all sources for the given time period.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean printData();

}
