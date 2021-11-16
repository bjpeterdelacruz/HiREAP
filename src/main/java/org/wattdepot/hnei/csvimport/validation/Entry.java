package org.wattdepot.hnei.csvimport.validation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
   * True if data for source at current timestamp is greater than data at previous timestamp, false
   * otherwise.
   */
  private boolean isMonotonicallyIncreasing;

  /**
   * Creates a new ReadingData object.
   * 
   * @param sourceName Name of source where power is consumed.
   * @param reading Energy consumed by a source.
   * @param timestamp Timestamp at which reading took place.
   * @param mtuId MTU ID.
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Entry(String sourceName, String reading, XMLGregorianCalendar timestamp, String mtuId) {
    this.sourceName = sourceName;
    this.timestamp = timestamp;
    this.reading = reading;
    this.mtuID = mtuId;
    this.isMonotonicallyIncreasing = true;
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
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public void setTimestamp(XMLGregorianCalendar timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Gets a timestamp.
   * 
   * @return Timestamp at which reading took place.
   */
  @SuppressFBWarnings("EI_EXPOSE_REP")
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
   * Sets isMonotonicallyIncreasing to true if data is monotonically increasing, false otherwise.
   * 
   * @param isMonotonicallyIncreasing True if data is monotonically increasing, false otherwise.
   */
  public void setMonotonicallyIncreasing(boolean isMonotonicallyIncreasing) {
    this.isMonotonicallyIncreasing = isMonotonicallyIncreasing;
  }

  /**
   * Returns true if data is monotonically increasing, false otherwise.
   * 
   * @return True if data is monotonically increasing, false otherwise.
   */
  public boolean isMonotonicallyIncreasing() {
    return isMonotonicallyIncreasing;
  }

  /**
   * Gets the String representation of an Entry object.
   * 
   * @return Source name, MTU ID, and timestamp.
   */
  public String toString() {
    String msg = "Source: " + this.sourceName + " -- MTU ID: " + this.mtuID;
    if (this.timestamp != null) {
      msg += " -- Reading: " + this.reading + " -- Timestamp: " + this.timestamp;
    }
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
    if ((this.reading == null || this.timestamp == null)
        || (e.reading == null || e.timestamp == null)) {
      return (this.sourceName.equals(e.sourceName) && this.mtuID.equals(e.mtuID));
    }
    else {
      return (this.sourceName.equals(e.sourceName) && this.reading.equals(e.reading)
          && this.timestamp.equals(e.timestamp) && this.mtuID.equals(e.mtuID));
    }
  }

  /**
   * Gets the hash code for an Entry object.
   * 
   * @return Hash code of Entry object.
   */
  public int hashCode() {
    int hashCode = 0;
    if (this.sourceName == null && this.reading == null && this.timestamp == null
        && this.mtuID == null) {
      return hashCode;
    }
    if (this.sourceName != null) {
      hashCode += this.sourceName.hashCode();
    }
    if (this.reading != null) {
      hashCode += this.reading.hashCode();
    }
    if (this.timestamp != null) {
      hashCode += this.timestamp.hashCode();
    }
    if (this.mtuID != null) {
      hashCode += this.mtuID.hashCode();
    }
    return hashCode;
  }

  /**
   * Compares two Entry objects together using names of sources, MTU IDs, and values of
   * <code>isMonotonicallyIncreasing</code>.
   * 
   * @param e Entry object to compare with.
   * @return 0 if equal, less than 0 if first argument is greater than second argument, greater than
   * 0 if vice versa
   */
  @Override
  public int compareTo(Entry e) {
    int result = this.sourceName.compareTo(e.sourceName);
    if (result == 0) {
      if ((result = this.mtuID.compareTo(e.mtuID)) == 0) {
        if (this.isMonotonicallyIncreasing && !e.isMonotonicallyIncreasing) {
          return 1;
        }
        else if (!this.isMonotonicallyIncreasing && e.isMonotonicallyIncreasing) {
          return -1;
        }
        else {
          return 0;
        }
      }
      else {
        return result;
      }
    }
    else {
      return result;
    }
  }

}
