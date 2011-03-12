package org.wattdepot.hnei.csvimport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to annotate the HoboImporter class.
 * 
 * @author BJ Peter DeLaCruz
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Hobo {
  /** Gets the value of name attribute. */
  public String name();
  /** Gets the value of value attribute. */
  public String value();
}
