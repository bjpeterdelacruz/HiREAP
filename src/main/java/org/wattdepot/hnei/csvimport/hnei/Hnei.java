package org.wattdepot.hnei.csvimport.hnei;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to annotate the HneiImporter class.
 * 
 * @author BJ Peter DeLaCruz
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Hnei {
  /** Gets the value of name attribute. */
  String name();
  /** Gets the value of value attribute. */
  String value();
}
