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

  /**
   * Verifies all data for all sources for a given time period.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean verifyData() {
    XMLGregorianCalendar dateBeforeStartDate = Tstamp.incrementDays(this.startTimestamp, -1);
    XMLGregorianCalendar dateAfterEndDate = Tstamp.incrementDays(this.endTimestamp, 1);

    // Run validation tests. First, test if all data points for a source are monotonically
    // increasing.
    MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();

    List<Source> sources = null;
    //
    //
    Set<Source> gradeASources = new HashSet<Source>();
    // Grade B sources include those that have missing data from dateBeforeStartDate to
    // startTimestamp and from endTimestamp to dateAfterEndDate.
    Set<Source> gradeBSources = new HashSet<Source>();
    // Grade C sources include those that have missing data within time interval and
    // non-monotonically increasing data.
    Set<Source> gradeCSources = new HashSet<Source>();

    List<SensorData> sensorDatas = null;
    Set<SensorData> invalidDatas = new HashSet<SensorData>();

    try {
      sources = this.client.getSources();
      int count = 1;
      for (Source s : sources) {
        System.out.print("Validating data for source " + s.getName());
        System.out.println(" [" + (count++) + " of " + sources.size() + "]...");

        sensorDatas =
            this.client.getSensorDatas(s.getName(), dateBeforeStartDate, dateAfterEndDate);
        if (sensorDatas.isEmpty()) {
          gradeCSources.add(s);
        }
        else {
          if (Tstamp.daysBetween(sensorDatas.get(0).getTimestamp(), this.startTimestamp) != 0
              && Tstamp.greaterThan(sensorDatas.get(0).getTimestamp(), this.startTimestamp)) {
            gradeCSources.add(s);
          }
          else if (Tstamp.daysBetween(sensorDatas.get(sensorDatas.size() - 1).getTimestamp(),
              this.endTimestamp) != 0
              && Tstamp.lessThan(sensorDatas.get(sensorDatas.size() - 1).getTimestamp(),
                  this.endTimestamp)) {
            gradeCSources.add(s);
          }
          else {
            validator.setDatas(sensorDatas);
            for (SensorData d : sensorDatas) {
              validator.setCurrentData(d);
              if (!validator.validateEntry(null)) {
                System.out.println("Source " + s.getName()
                    + " has non-monotonically increasing data...");
                invalidDatas.add(d);
                gradeCSources.add(s);
              }
            }

            sensorDatas =
                this.client.getSensorDatas(s.getName(), dateBeforeStartDate, this.startTimestamp);
            if (sensorDatas.isEmpty()) {
              gradeBSources.add(s);
            }
            else {
              sensorDatas =
                  this.client.getSensorDatas(s.getName(), this.endTimestamp, dateAfterEndDate);
              if (sensorDatas.isEmpty()) {
                gradeBSources.add(s);
              }
              else {
                gradeASources.add(s);
              }
            }
          }
        }
      }
    }
    catch (WattDepotClientException e) {
      return false;
    }

    if (!invalidDatas.isEmpty()) {
      for (SensorData d : sensorDatas) {
        System.out.println(d.getSource() + ": "
            + d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE));
      }
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
    if (!classifier.setup()) {
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!classifier.getAllSources()) {
      System.exit(1);
    }

    if (!classifier.getDates(br) || !classifier.verifyData()) {
      System.exit(1);
    }

  }

}
