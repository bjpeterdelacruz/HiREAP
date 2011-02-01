package org.wattdepot.hnei.csvimport.validation;

/**
 * Used by HneiImporter class to grab all sources that have multiple MTU IDs.
 * 
 * @author BJ Peter DeLaCruz
 */
public class SourceMtu implements Comparable<SourceMtu> {

  /** Name of a source. */
  private String sourceName;

  /** MTU ID. */
  private String mtuID;

  /**
   * Creates a new SourceMtu object.
   * 
   * @param sourceName Name of a source.
   * @param mtuID MTU ID.
   */
  public SourceMtu(String sourceName, String mtuID) {
    this.sourceName = sourceName;
    this.mtuID = mtuID;
  }

  /**
   * Sets the name of a source to the given value.
   * 
   * @param sourceName Name of a source.
   */
  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  /**
   * Gets the name of a source.
   * 
   * @return Name of a source.
   */
  public String getSourceName() {
    return sourceName;
  }

  /**
   * Sets the MTU ID to the given value.
   * 
   * @param mtuID MTU ID.
   */
  public void setMtuID(String mtuID) {
    this.mtuID = mtuID;
  }

  /**
   * Gets the MTU ID.
   * 
   * @return MTU ID.
   */
  public String getMtuID() {
    return mtuID;
  }

  /**
   * Gets the String representation of a SourceMtu object.
   * 
   * @return Source name and MTU ID.
   */
  public String toString() {
    return "Source: " + this.sourceName + " -- MTU ID: " + this.mtuID;
  }

  /**
   * Used to test if two SourceMtu objects are the same.
   * 
   * @param o SourceMtu object to compare with.
   * @return True if both objects are equal, false otherwise.
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceMtu sourceMtu = (SourceMtu) o;
    return (this.sourceName.equals(sourceMtu.sourceName) && this.mtuID.equals(sourceMtu.mtuID));
  }

  /**
   * Gets the hash code for a SourceMtu object.
   * 
   * @return Hash code of SourceMtu object.
   */
  public int hashCode() {
    int hashCode = 0;
    if (this.sourceName == null && this.mtuID == null) {
      return hashCode;
    }
    else if (this.sourceName == null) {
      return this.mtuID.hashCode();
    }
    else if (this.mtuID == null) {
      return this.sourceName.hashCode();
    }
    else {
      return this.sourceName.hashCode() + this.mtuID.hashCode();
    }
  }

  /**
   * Used to compare two SourceMtu objects together.
   * 
   * @param sourceMtu SourceMtu object to compare with.
   * @return 0 if equal, less than 0 if first argument is greater than second argument, greater than
   * 0 if vice versa
   */
  @Override
  public int compareTo(SourceMtu sourceMtu) {
    int result = this.sourceName.compareTo(sourceMtu.sourceName);
    if (result == 0) {
      return this.mtuID.compareTo(sourceMtu.mtuID);
    }
    else {
      return result;
    }
  }

}
