package org.wattdepot.hnei.export;

import java.io.BufferedWriter;

/**
 * 
 * 
 * @author BJ Peter DeLaCruz
 */
public interface Exportable {

  /**
   * Prints information in SensorData objects to a CSV file.
   * 
   * @param writer CSV file to write data to.
   * @return True if successful, false otherwise.
   */
  public boolean printFields(BufferedWriter writer);

}
