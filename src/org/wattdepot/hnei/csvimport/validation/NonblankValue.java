package org.wattdepot.hnei.csvimport.validation;

/**
 * A Validator that checks to make sure that an entry is non-blank.
 * 
 * @author BJ Peter DeLaCruz
 */
public class NonblankValue implements Validator {

  /** Error message. */
  private static final String errorMessage = "Entry cannot be blank.";

  /**
   * Checks to make sure that an entry is non-blank.
   * 
   * @param entry The entry to validate.
   * @return True if entry is non-blank, false otherwise.
   */
  @Override
  public boolean validateEntry(Object entry) {
    String str = (String) entry;
    if (str == null) {
      return false;
    }
    return !str.isEmpty();
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
