package org.wattdepot.hnei.csvimport;

/**
 * A Validator that checks to make sure that an entry is a valid number.
 * 
 * @author BJ Peter DeLaCruz
 */
public class NumericValue implements Validator {

  /** Error message. */
  private static final String errorMessage = "Entry is not a valid number.";

  /**
   * Checks to make sure that an entry is a valid number.
   * 
   * @param entry The entry to validate.
   * @return True if entry is a valid number, false otherwise.
   */
  @Override
  public boolean validateEntry(Object entry) {
    String str = (String) entry;
    try {
      Double.parseDouble(str);
    }
    catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  /**
   * Returns a string explaining why validation failed for an entry.
   * 
   * @return An error message.
   */
  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

}
