package org.wattdepot.hnei.export;

import java.io.Serializable;
import java.util.Comparator;
import org.wattdepot.hnei.csvimport.SamplingInterval;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * A comparator for WattDepot sources.
 * 
 * @author BJ Peter DeLaCruz
 */
public class SourceComparator implements Comparator<Source>, Serializable {

  /**
   * Used to serialize this class.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Sorts the sources stored on the WattDepot server based on the type of data (daily, hourly, or
   * N/A).
   * 
   * @param s1 First source.
   * @param s2 Second source.
   * @return 0 if s1 == s2, -1 if s1 > s2, and 1 if s1 < s2.
   */
  @Override
  public int compare(Source s1, Source s2) {
    String property1 = s1.getProperties().getProperty(SamplingInterval.SAMPLING_INTERVAL);
    String property2 = s2.getProperties().getProperty(SamplingInterval.SAMPLING_INTERVAL);
    return property1.compareTo(property2);
  }

}
