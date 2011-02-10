package org.wattdepot.hnei.csvimport;

/**
 * Classes that implement this interface will import all kinds of data from CSV files.
 * 
 * @author BJ Peter DeLaCruz
 */
public interface Importer {

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  public void printStats();

  /**
   * Parses each row, creates a SensorData object from each, and stores the sensor data on a
   * WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean processCsvFile();

}
