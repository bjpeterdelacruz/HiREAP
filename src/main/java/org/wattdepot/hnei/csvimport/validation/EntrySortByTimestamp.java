package org.wattdepot.hnei.csvimport.validation;

import java.io.Serializable;
import java.util.Comparator;

/**
 * This class is used to sort Entry objects.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EntrySortByTimestamp implements Comparator<Entry>, Serializable {

  /**
   * Used when serializing instances of this class.
   */
  private static final long serialVersionUID = 160015231074789691L;

  /**
   * Compares the timestamps in two Entry objects.
   * 
   * @param e1 First Entry object.
   * @param e2 Second Entry object.
   * @return Less than 0 if the timestamp in e1 is greater than the timestamp in e2, 0 if both are
   * equal, and greater than 0 if the timestamp in e1 is less than the timestamp in e2.
   */
  @Override
  public int compare(Entry e1, Entry e2) {
    return e2.getTimestamp().toString().compareTo(e1.getTimestamp().toString());
  }

}
