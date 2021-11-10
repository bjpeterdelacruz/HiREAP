package org.wattdepot.hnei.export.cli;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Finds all sources whose sensor data are not monotonically increasing for a given time period and
 * then returns a string of those sources along with timestamps for those data.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HireapDataValidator {

  /** Used to retrieve sources and data on WattDepot server. */
  protected WattDepotClient client;

  /**
   * Reads in URI, username, and password from a properties file, connects to a WattDepot server,
   * and then stores a test source.
   * 
   * @return true if connection is established, false otherwise.
   */
  public boolean setup() {
    DataInputClientProperties props;
    try {
      props = new DataInputClientProperties();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    var uri = props.get(WATTDEPOT_URI_KEY);
    var username = props.get(WATTDEPOT_USERNAME_KEY);
    var password = props.get(WATTDEPOT_PASSWORD_KEY);

    this.client = new WattDepotClient(uri, username, password);
    if (!this.client.isAuthenticated() || !this.client.isHealthy()) {
      System.out.println("Is authenticated? " + this.client.isAuthenticated());
      System.out.println("Is healthy? " + this.client.isHealthy());
      return false;
    }
    return true;
  }

  /**
   * Returns a string of sources whose sensor data are not monotonically increasing, along with the
   * timestamps for those data.
   * 
   * @param startTime Start date.
   * @param endTime End date.
   * @return String of sources and timestamps.
   */
  public String validateData(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime) {
    try {
      var sources = this.client.getSources();
      var validator = new MonotonicallyIncreasingValue();

      var msg = "Source,Timestamp\n";
      var builder = new StringBuilder();
      builder.append(msg);

      for (int idx = 0; idx < sources.size(); idx++) {
        var datas = this.client.getSensorDatas(sources.get(idx).getName(), startTime, endTime);
        System.out.print("Processing " + sources.get(idx).getName() + "... [" + (idx + 1) + " ");
        System.out.println("of " + sources.size() + "]");
        validator.setDatas(datas);
        System.out.println("Number of data: " + datas.size());
        for (SensorData d : datas) {
          var entry = new Entry(sources.get(idx).getName(), null, d.getTimestamp(), null);
          validator.setCurrentData(d);
          if (!validator.validateEntry(entry)) {
            msg = entry.getSourceName() + "," + entry.getTimestamp() + "\n";
            builder.append(msg);
          }
        }
      }
      return builder.toString();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Prints a string of sources whose sensor data are not monotonically increasing, along with the
   * timestamps for those data to a CSV file.
   * 
   * @param startTime Start date.
   * @param endTime End date.
   * @return True if successful, false otherwise.
   */
  public boolean printData(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime) {
    var outputFilename = startTime + "_" + endTime + ".csv";
    var outputFile = new File(outputFilename);
    if (!outputFile.setWritable(true)) {
      return false;
    }

    try (var writer = new BufferedWriter(new FileWriter(outputFile, StandardCharsets.UTF_8))) {
      var result = this.validateData(startTime, endTime);
      if (result == null) {
        throw new IOException();
      }

      writer.write(result);
      System.out.println(result);
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  /**
   * A command-line program that finds all sources whose sensor data are not monotonically
   * increasing for a given time period and then returns a string of those sources along with
   * timestamps for those data.
   * 
   * @param args None.
   */
  public static void main(String[] args) {
    var validator = new HireapDataValidator();
    if (!validator.setup()) {
      System.exit(1);
    }

    try {
      var startTime = Tstamp.makeTimestamp("2010-01-01T06:00:00.000-10:00");
      var endTime = Tstamp.makeTimestamp("2011-04-01T06:00:00.000-10:00");

      if (!validator.printData(startTime, endTime)) {
        System.exit(1);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
