package org.wattdepot.hnei.csvimport.validation;

import javax.xml.datatype.XMLGregorianCalendar;

/**
 * Used by MonotonicallyIncreasingValue class to validate entry.
 * 
 * @author BJ Peter DeLaCruz
 */
public class Entry {

  /** Source of power consumed. */
  private String sourceName;

  /** Power consumed. */
  private String reading;

  /** Timestamp at which reading took place. */
  private XMLGregorianCalendar timestamp;

  /** MTU ID. */
  private String mtuId;

  /**
   * Creates a new ReadingData object.
   * 
   * @param sourceName Name of source where power is consumed.
   * @param timestamp Timestamp at which reading took place.
   * @param reading Power consumed by a source.
   * @param mtuId MTU ID.
   */
  public Entry(String sourceName, String reading, XMLGregorianCalendar timestamp, String mtuId) {
    this.sourceName = sourceName;
    this.timestamp = timestamp;
    this.reading = reading;
    this.mtuId = mtuId;
  }

  /**
   * Sets the name of a source to a given value.
   * 
   * @param sourceName Name of source where power is consumed.
   */
  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  /**
   * Gets the name of a source.
   * 
   * @return Name of source where power is consumed.
   */
  public String getSourceName() {
    return sourceName;
  }

  /**
   * Sets a reading to a given value.
   * 
   * @param reading Power consumed.
   */
  public void setReading(String reading) {
    this.reading = reading;
  }

  /**
   * Gets a reading.
   * 
   * @return Power consumed.
   */
  public String getReading() {
    return reading;
  }

  /**
   * Sets a timestamp to a given value.
   * 
   * @param timestamp Timestamp at which reading took place.
   */
  public void setTimestamp(XMLGregorianCalendar timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Gets a timestamp.
   * 
   * @return Timestamp at which reading took place.
   */
  public XMLGregorianCalendar getTimestamp() {
    return timestamp;
  }

  /**
   * Sets the MTU ID to a given value.
   * 
   * @param mtuId MTU ID.
   */
  public void setMtuId(String mtuId) {
    this.mtuId = mtuId;
  }

  /**
   * Gets the MTU ID.
   * 
   * @return MTU ID.
   */
  public String getMtuId() {
    return mtuId;
  }

}
