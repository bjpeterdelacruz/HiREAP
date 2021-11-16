package org.wattdepot.hnei.csvimport.validation;

/**
 * Classes that implement this interface will validate an entry based on a condition. For example,
 * a NumericValue class that implements this interface will check to see if an entry is a valid
 * number.
 * 
 * @author BJ Peter DeLaCruz
 */
public interface Validator {

  /**
   * Validates an entry (in a row) from a CSV file.
   * 
   * @param entry The entry to validate.
   * @return True if entry passes validation, false otherwise.
   */
  boolean validateEntry(Object entry);

  /**
   * Returns a string explaining why validation failed for an entry.
   * 
   * @return An error message.
   */
  String getErrorMessage();
}
