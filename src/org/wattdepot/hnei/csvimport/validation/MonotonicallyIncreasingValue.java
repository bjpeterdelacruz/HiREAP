package org.wattdepot.hnei.csvimport.validation;

import java.util.List;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * A Validator that checks to make sure that the value of an entry is monotonically increasing.
 * 
 * @author BJ Peter DeLaCruz
 */
public class MonotonicallyIncreasingValue implements Validator {

  /** Sensor data for a source at a previous timestamp. */
  private SensorData previousData;

  /** Sensor data for a source at the current timestamp. */
  private SensorData currentData;

  /** List of sensor data for a source for a given time period. */
  private List<SensorData> datas;

  /**
   * Sets the list of sensor data.
   * 
   * @param datas The list of sensor data.
   */
  public void setDatas(List<SensorData> datas) {
    this.datas = datas;
  }

  /**
   * Sets the current sensor data to be processed.
   * 
   * @param data The current sensor data to be processed.
   */
  public void setCurrentData(SensorData data) {
    this.currentData = data;
  }

  /**
   * Given a timestamp and reading, this validator checks to make sure that the reading at the last
   * timestamp is not greater than nor equal to the reading at the timestamp that was passed in to
   * this validator.
   * 
   * @param entry The entry to validate.
   * @return True if the entry at the current timestamp is less than the entry at the previous
   * timestamp, false otherwise.
   */
  @Override
  public boolean validateEntry(Object entry) {
    if (this.datas.size() < 2 || this.datas.indexOf(this.currentData) == 0) {
      // No other data exist but the data at the current timestamp, so return.
      return true;
    }

    this.previousData = this.datas.get(this.datas.indexOf(this.currentData) - 1);
    double currReading = this.currentData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    double prevReading =
      this.previousData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    return currReading >= prevReading;
  }

  /**
   * Returns a string explaining why validation failed for an entry.
   * 
   * @return An error message.
   */
  @Override
  public String getErrorMessage() {
    String reading = "reading";
    String errorMessage = "The reading at " + this.previousData.getTimestamp();
    errorMessage += " (" + this.previousData.getProperty(reading) + ") is greater than or equal ";
    errorMessage += "to the reading at " + this.currentData.getTimestamp() + " (";
    errorMessage += this.currentData.getProperty(reading) + ") for ";
    errorMessage += this.currentData.getSource() + ".";
    return errorMessage;
  }
}
