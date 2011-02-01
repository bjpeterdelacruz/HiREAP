package org.wattdepot.hnei.csvimport.validation;

import javax.xml.datatype.XMLGregorianCalendar;

/**
 * Used by MonotonicallyIncreasingValue class to validate entry.
 * 
 * @author BJ Peter DeLaCruz
 */
public class Entry implements Comparable<Entry> {

  /** Source of power consumed. */
  private String sourceName;

  /** Power consumed. */
  private String reading;

  /** Timestamp at which reading took place. */
  private XMLGregorianCalendar timestamp;

  /** MTU ID. */
  private String mtuID;

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
    this.mtuID = mtuId;
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
    this.mtuID = mtuId;
  }

  /**
   * Gets the MTU ID.
   * 
   * @return MTU ID.
   */
  public String getMtuId() {
    return mtuID;
  }

  /**
   * Gets the String representation of an Entry object.
   * 
   * @return Source name, MTU ID, and timestamp.
   */
  public String toString() {
    String msg = "Source: " + this.sourceName + " -- MTU ID: " + this.mtuID;
    msg += " -- Timestamp: " + this.timestamp;
    return msg;
  }

  /**
   * Used to test if two Entry objects are the same.
   * 
   * @param o Entry object to compare with.
   * @return True if both objects are equal, false otherwise.
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entry e = (Entry) o;
    return (this.sourceName.equals(e.sourceName) && this.mtuID.equals(e.mtuID)
        && this.reading.equals(e.reading) && this.timestamp.equals(e.timestamp));
  }

  /**
   * Gets the hash code for an Entry object.
   * 
   * @return Hash code of Entry object.
   */
  public int hashCode() {
    int hashCode = 0;
    if (this.sourceName == null && this.reading == null && this.timestamp == null) {
      return hashCode;
    }
    else if (this.timestamp == null) {
      return this.sourceName.hashCode() + this.reading.hashCode();
    }
    else if (this.reading == null) {
      return this.sourceName.hashCode() + this.timestamp.hashCode();
    }
    else if (this.sourceName == null) {
      return this.reading.hashCode() + this.timestamp.hashCode();
    }
    else {
      return this.sourceName.hashCode() + this.reading.hashCode() + this.timestamp.hashCode();
    }
  }

  /**
   * Used to compare two Entry objects together.
   * 
   * @param e Entry object to compare with.
   * @return 0 if equal, less than 0 if first argument is greater than second argument, greater than
   * 0 if vice versa
   */
  @Override
  public int compareTo(Entry e) {
    int result = this.timestamp.toString().compareTo(e.timestamp.toString());
    if (result == 0) {
      return this.sourceName.compareTo(e.sourceName);
    }
    else {
      return result;
    }
  }

}
