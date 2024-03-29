package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
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

  /** */
  protected String gradeA_HourlyFilename;

  /** */
  protected String gradeA_DailyFilename;

  /** */
  protected String gradeB_Filename;

  /** */
  protected String gradeC_Filename;

  /**
   * Grade A sources do not include invalid data and contain energy data from dateBeforeStartDate to
   * dateAfterEndDate.
   */
  protected List<String> gradeA_DailySources;

  /**
   * Grade A sources do not include invalid data and contain energy data from dateBeforeStartDate to
   * dateAfterEndDate.
   */
  protected List<String> gradeA_HourlySources;

  /**
   * Grade B sources include those that have missing data from dateBeforeStartDate to startTimestamp
   * and from endTimestamp to dateAfterEndDate.
   */
  protected List<String> gradeB_Sources;

  /**
   * Grade C sources include those that have missing data within time interval and non-monotonically
   * increasing data.
   */
  protected List<String> gradeC_Sources;

  /**
   * Creates a new QualityClassifier object.
   */
  public QualityClassifier() {
    this.log = Logger.getLogger(QualityClassifier.class.getName());
    this.toolName = "QualityClassifier";
    this.gradeA_DailyFilename = "gradeA_daily";
    this.gradeA_HourlyFilename = "gradeA_hourly";
    this.gradeB_Filename = "gradeB";
    this.gradeC_Filename = "gradeC";
    this.sources = new ArrayList<>();
    this.gradeA_DailySources = new ArrayList<>();
    this.gradeA_HourlySources = new ArrayList<>();
    this.gradeB_Sources = new ArrayList<>();
    this.gradeC_Sources = new ArrayList<>();
  }

  /**
   * Verifies all sensor data for all sources for a given time period.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean verifyData() {
    this.dateBeforeStartDate = Tstamp.incrementDays(this.startTimestamp, -1);
    this.dateAfterEndDate = Tstamp.incrementDays(this.endTimestamp, 1);

    MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();

    int totalDailySources = 0;
    int totalHourlySources = 0;
    int numIncompleteDailyData = 0;
    // int numIncompleteHourlyData = 0;
    Set<SensorData> invalidDatas = new HashSet<>();

    try {
      int count = 1;
      for (Source s : this.sources) {
        if (s.getProperty(SamplingInterval.SAMPLING_INTERVAL) != null
            && s.getProperty(SamplingInterval.SAMPLING_INTERVAL).equals(SamplingInterval.DAILY)) {
          totalDailySources++;
        }
        else if (s.getProperty(SamplingInterval.SAMPLING_INTERVAL) != null) {
          totalHourlySources++;
        }

        System.out.print("Validating data for source " + s.getName());
        System.out.println(" [" + (count++) + " of " + this.sources.size() + "]...");

        var sensorDatas =
            this.client
                .getSensorDatas(s.getName(), this.dateBeforeStartDate, this.dateAfterEndDate);

        if (sensorDatas.isEmpty()) {
          String msg = "No data exists for source " + s.getName() + " between ";
          msg += this.dateBeforeStartDate + " and " + this.dateAfterEndDate + ".\n\n";
          this.gradeC_Sources.add(s.getName() + "\tNo data exists between time interval.");
          this.log.log(Level.INFO, msg);
          continue;
        }

        XMLGregorianCalendar timestamp = sensorDatas.get(0).getTimestamp();

        // If there is some missing data, e.g. start timestamp is May 2, 2011, but first timestamp
        // is May 5, 2011, flag the source as Grade C.
        if (Tstamp.daysBetween(timestamp, this.startTimestamp) != 0
            && Tstamp.greaterThan(timestamp, this.startTimestamp)) {
          String msg = "Missing data for source " + s.getName() + ".\n";
          msg += "  Start timestamp: " + this.startTimestamp + "\n";
          msg += "  First timestamp: " + timestamp + "\n" + "\n";
          this.gradeC_Sources.add(s.getName()
              + "\tMissing some data points after start date. First timestamp: " + timestamp + ".");
          this.log.log(Level.INFO, msg);
          continue;
        }

        timestamp = sensorDatas.get(sensorDatas.size() - 1).getTimestamp();

        // For example, end timestamp is May 31, 2011, but last timestamp is May 20, 2011.
        if (Tstamp.daysBetween(timestamp, this.endTimestamp) != 0
            && Tstamp.lessThan(timestamp, this.endTimestamp)) {
          String msg = "Missing data for source " + s.getName() + ".\n";
          msg += "  End timestamp: " + this.endTimestamp + "\n";
          msg += "  Last timestamp: " + timestamp + "\n" + "\n";
          this.gradeC_Sources.add(s.getName()
              + "\tMissing some data points before end date. Last timestamp: " + timestamp + ".");
          this.log.log(Level.INFO, msg);
          continue;
        }

        // Test if all data points for a source are non-monotonically increasing. If a data
        // point is not, then add source to list of Grade C sources.
        boolean containsInvalidData = false;
        validator.setDatas(sensorDatas);
        for (SensorData d : sensorDatas) {
          validator.setCurrentData(d);
          if (!validator.validateEntry(null)) {
            invalidDatas.add(d);
            this.gradeC_Sources.add(s.getName()
                + "\tContains data that are not monotonically increasing.");
            containsInvalidData = true;
            break;
          }
        }

        if (containsInvalidData) {
          StringBuilder builder = new StringBuilder();
          String msg = "Source " + s.getName();
          msg += " contains data that are not monotonically increasing:\n";
          builder.append(msg);
          for (SensorData d : invalidDatas) {
            msg = "  Timestamp: " + d.getTimestamp() + " -- Energy (kWh): ";
            builder.append(msg);
            msg = d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE) + "\n";
            builder.append(msg);
          }
          this.log.log(Level.INFO, builder.toString());
          invalidDatas.clear();
          continue;
        }

        // Verify that data exists before start timestamp and also after end timestamp so
        // that WattDepot can interpolate data for given time interval.
        timestamp = Tstamp.incrementSeconds(this.startTimestamp, -1);

        List<SensorData> beforeSensorDatas =
            this.client.getSensorDatas(s.getName(), this.dateBeforeStartDate, timestamp);

        if (beforeSensorDatas.isEmpty()) {
          String msg = "No data exists for source " + s.getName() + " between ";
          msg += this.dateBeforeStartDate + " and " + timestamp + ".\n\n";
          this.gradeB_Sources.add(s.getName() + "\tNo data points exist before " + timestamp + ".");
          this.log.log(Level.INFO, msg);
          continue;
        }

        timestamp = Tstamp.incrementSeconds(this.endTimestamp, 1);

        List<SensorData> afterSensorDatas =
            this.client.getSensorDatas(s.getName(), timestamp, this.dateAfterEndDate);

        if (afterSensorDatas.isEmpty()) {
          String msg = "No data exists for source " + s.getName() + " between ";
          msg += timestamp + " and " + this.dateAfterEndDate + ".\n\n";
          this.gradeB_Sources.add(s.getName() + "\tNo data points exist after " + timestamp + ".");
          this.log.log(Level.INFO, msg);
          continue;
        }

        // If source contains daily data, verify that there is only one data point per day for
        // each day in time interval.
        if (s.getProperty(SamplingInterval.SAMPLING_INTERVAL).equals(SamplingInterval.DAILY)) {
          int expectedNumDailyData =
              Tstamp.daysBetween(this.dateBeforeStartDate, this.dateAfterEndDate) + 1;
          if (expectedNumDailyData == sensorDatas.size()) {
            this.gradeA_DailySources.add(s.getName());
          }
          else {
            numIncompleteDailyData++;
            String msg = "Number of daily data for source " + s.getName() + " is not equal to ";
            msg += expectedNumDailyData + " [number of daily data found: ";
            msg += sensorDatas.size() + "].\n\n";
            this.gradeB_Sources.add(s.getName() + "\tExpected " + expectedNumDailyData
                + " data points. Found " + sensorDatas.size() + ".");
            this.log.log(Level.INFO, msg);
          }
        }
        // TODO: If source contains hourly data...
        else {
          this.gradeA_HourlySources.add(s.getName());
        }
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    printStats(totalDailySources, totalHourlySources, numIncompleteDailyData);

    String timeInterval =
        this.startTimestamp.getYear() + "." + this.startTimestamp.getMonth() + "."
            + this.startTimestamp.getDay();
    timeInterval +=
        "-" + this.endTimestamp.getYear() + "." + this.endTimestamp.getMonth() + "."
            + this.endTimestamp.getDay() + ".txt";

    Collections.sort(this.gradeA_DailySources);
    Collections.sort(this.gradeA_HourlySources);
    Collections.sort(this.gradeB_Sources);
    Collections.sort(this.gradeC_Sources);

    String filename = this.gradeA_DailyFilename + "-" + timeInterval;
    if (!writeResultsToFile(filename, this.gradeA_DailySources)) {
      return false;
    }

    filename = this.gradeA_HourlyFilename + "-" + timeInterval;
    if (!writeResultsToFile(filename, this.gradeA_HourlySources)) {
      return false;
    }

    filename = this.gradeB_Filename + "-" + timeInterval;
    if (!writeResultsToFile(filename, this.gradeB_Sources)) {
      return false;
    }

    filename = this.gradeC_Filename + "-" + timeInterval;
    return writeResultsToFile(filename, this.gradeC_Sources);
  }

  /**
   * Prints some statistics such as the number of Grade A daily sources to a log file and also to
   * the screen.
   * 
   * @param totalDailySources Total number of daily sources.
   * @param totalHourlySources Total number of hourly sources.
   * @param numIncompleteDailyData Number of daily sources that are missing some data.
   */
  public void printStats(int totalDailySources, int totalHourlySources,
                         int numIncompleteDailyData) {
    String msg = "Grade A sources (daily):\n-----------------\nTotal: ";
    msg += this.gradeA_DailySources.size() + "\n" + "\n";
    this.log.log(Level.INFO, msg);
    System.out.print(msg);

    msg = "Grade A sources (hourly):\n-----------------\nTotal: ";
    msg += this.gradeA_HourlySources.size() + "\n" + "\n";
    this.log.log(Level.INFO, msg);
    System.out.print(msg);

    int total = this.gradeA_DailySources.size() + this.gradeA_HourlySources.size();
    msg = "Total number of Grade A sources: " + total + "\n" + "\n";
    this.log.log(Level.INFO, msg);
    System.out.print(msg);

    msg = "Grade B sources:\n-----------------\nTotal: ";
    msg += this.gradeB_Sources.size() + "\n" + "\n";
    this.log.log(Level.INFO, msg);
    System.out.print(msg);

    msg = "Grade C sources:\n-----------------\nTotal: ";
    msg += this.gradeC_Sources.size() + "\n" + "\n";
    this.log.log(Level.INFO, msg);
    System.out.print(msg);

    msg = "Number of daily sources that are missing some data: " + numIncompleteDailyData;
    msg += String.format("%n%nTotal number of daily sources:  %5d%n", totalDailySources);
    msg += String.format("Total number of hourly sources: %5d%n", totalHourlySources);
    msg += String.format("Total number of sources:        %5d%n%n", (totalDailySources + totalHourlySources));

    this.log.log(Level.INFO, msg);
    System.out.print(msg);
  }

  /**
   * Writes the time interval and a list of sources (Grades A, B, or C) to a text file.
   * 
   * @param outputFilename Name of output file.
   * @param sources List of all sources stored on the WattDepot server.
   * @return True if successful, false otherwise.
   */
  public boolean writeResultsToFile(String outputFilename, List<String> sources) {
    var outputFile = new File(outputFilename);
    if (!outputFile.setWritable(true)) {
      return false;
    }

    try (var writer = new BufferedWriter(new FileWriter(outputFile, StandardCharsets.UTF_8))) {
      writer.write(this.startTimestamp + "\n");
      writer.write(this.endTimestamp + "\n\n");

      for (String s : sources) {
        writer.write(s + "\n");
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
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
    var classifier = new QualityClassifier();
    if (!classifier.setup() || !classifier.setupLogger() || !classifier.getAllSources()) {
      System.exit(1);
    }

    var br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    if (!classifier.getDates(br) || !classifier.verifyData() || !classifier.closeLogger()) {
      System.exit(1);
    }
  }

}
