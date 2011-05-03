package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This program verifies that all data for all sources for a given time period are valid.
 * 
 * @author BJ Peter DeLaCruz
 */
public class QualityClassifier extends EnergyMatrixExporter {

  /** */
  protected XMLGregorianCalendar dateBeforeStartDate;

  /** */
  protected XMLGregorianCalendar dateAfterEndDate;

  /**
   * Grade A sources do not include invalid data and contain energy data from dateBeforeStartDate to
   * dateAfterEndDate.
   */
  protected Set<Source> gradeASources;

  /**
   * Grade B sources include those that have missing data from dateBeforeStartDate to startTimestamp
   * and from endTimestamp to dateAfterEndDate.
   */
  protected Set<Source> gradeBSources;

  /**
   * Grade C sources include those that have missing data within time interval and non-monotonically
   * increasing data.
   */
  protected Set<Source> gradeCSources;

  /**
   * Creates a new QualityClassifier object.
   */
  public QualityClassifier() {
    this.gradeASources = new HashSet<Source>();
    this.gradeBSources = new HashSet<Source>();
    this.gradeCSources = new HashSet<Source>();
  }

  /**
   * Verifies all data for all sources for a given time period.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean verifyData() {
    this.dateBeforeStartDate = Tstamp.incrementDays(this.startTimestamp, -1);
    this.dateAfterEndDate = Tstamp.incrementDays(this.endTimestamp, 1);

    MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();

    List<Source> sources = null;
    List<SensorData> sensorDatas = null;
    Set<SensorData> invalidDatas = new HashSet<SensorData>();

    try {
      sources = this.client.getSources();
      int count = 1;
      for (Source s : sources) {
        System.out.print("Validating data for source " + s.getName());
        System.out.println(" [" + (count++) + " of " + sources.size() + "]...");

        sensorDatas =
            this.client
                .getSensorDatas(s.getName(), this.dateBeforeStartDate, this.dateAfterEndDate);

        if (sensorDatas.isEmpty()) {
          this.gradeCSources.add(s);
        }
        else {
          // If there is some missing data, e.g. start timestamp is May 2, 2011, but first timestamp
          // is May 5, 2011, flag the source as Grade C.
          if (Tstamp.daysBetween(sensorDatas.get(0).getTimestamp(), this.startTimestamp) != 0
              && Tstamp.greaterThan(sensorDatas.get(0).getTimestamp(), this.startTimestamp)) {
            this.gradeCSources.add(s);
          }

          XMLGregorianCalendar timestamp = sensorDatas.get(sensorDatas.size() - 1).getTimestamp();

          // For example, end timestamp is May 31, 2011, but last timestamp is May 20, 2011.
          if (Tstamp.daysBetween(timestamp, this.endTimestamp) != 0
              && Tstamp.lessThan(timestamp, this.endTimestamp)) {
            this.gradeCSources.add(s);
          }
          else {
            // Test if all data points for a source are non-monotonically increasing. If a data
            // point is not, then add source to list of Grade C sources.
            validator.setDatas(sensorDatas);
            for (SensorData d : sensorDatas) {
              validator.setCurrentData(d);
              if (!validator.validateEntry(null)) {
                String msg = "Source " + s.getName() + " has non-monotonically increasing data...";
                System.out.println(msg);
                invalidDatas.add(d);
                this.gradeCSources.add(s);
              }
            }

            // If a source contains daily data, make sure there is at least one data point per day
            // in given time interval.
            

            // Verify that data exists before start timestamp and also after end timestamp so
            // that WattDepot can interpolate data for given time interval.
            sensorDatas =
                this.client.getSensorDatas(s.getName(), this.dateBeforeStartDate,
                    this.startTimestamp);

            if (sensorDatas.isEmpty()) {
              this.gradeBSources.add(s);
            }
            else {
              sensorDatas =
                  this.client.getSensorDatas(s.getName(), this.endTimestamp, this.dateAfterEndDate);

              if (sensorDatas.isEmpty()) {
                this.gradeBSources.add(s);
              }
              else {
                this.gradeASources.add(s);
              }
            }
          }
        }
      }
    }
    catch (WattDepotClientException e) {
      return false;
    }

    System.out.println("Grade B sources:\n-----------------");
    for (Source s : this.gradeBSources) {
      System.out.println(s.getName());
    }

    System.out.println("Grade C sources:\n-----------------");
    for (Source s : this.gradeCSources) {
      System.out.println(s.getName());
    }

    return true;
  }

  /**
   * Command-line program that will generate a CSV file containing energy information for one or
   * more sources over a given time period and at a given sampling interval.
   * 
   * @param args One argument to specify whether data for all sources should be exported.
   */
  public static void main(String[] args) {
    QualityClassifier classifier = new QualityClassifier();
    if (!classifier.setup() && !classifier.getAllSources()) {
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!classifier.getDates(br) || !classifier.verifyData()) {
      System.exit(1);
    }
  }

}
