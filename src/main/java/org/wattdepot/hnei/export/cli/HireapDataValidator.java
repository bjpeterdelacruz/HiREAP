package org.wattdepot.hnei.export.cli;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
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
    DataInputClientProperties props = null;
    try {
      props = new DataInputClientProperties();
    }
    catch (IOException e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }

    String uri = props.get(WATTDEPOT_URI_KEY);
    String username = props.get(WATTDEPOT_USERNAME_KEY);
    String password = props.get(WATTDEPOT_PASSWORD_KEY);

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
      List<Source> sources = this.client.getSources();
      List<SensorData> datas = null;
      Entry entry = null;
      MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();

      String msg = "Source,Timestamp\n";
      StringBuffer buffer = new StringBuffer();
      buffer.append(msg);

      for (int idx = 0; idx < sources.size(); idx++) {
        datas = this.client.getSensorDatas(sources.get(idx).getName(), startTime, endTime);
        System.out.print("Processing " + sources.get(idx).getName() + "... [" + (idx + 1) + " ");
        System.out.println("of " + sources.size() + "]");
        validator.setDatas(datas);
        System.out.println("Number of data: " + datas.size());
        for (SensorData d : datas) {
          entry = new Entry(sources.get(idx).getName(), null, d.getTimestamp(), null);
          validator.setCurrentData(d);
          if (!validator.validateEntry(entry)) {
            msg = entry.getSourceName() + "," + entry.getTimestamp() + "\n";
            buffer.append(msg);
          }
        }
      }
      return buffer.toString();
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
    String outputFilename = startTime + "_" + endTime + ".csv";
    File outputFile = new File(outputFilename);
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    boolean success = true;

    try {
      writer = new BufferedWriter(new FileWriter(outputFile));

      String result = this.validateData(startTime, endTime);
      if (result == null) {
        throw new IOException();
      }

      writer.write(result);
      System.out.println(result);
    }
    catch (IOException e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      try {
        writer.close();
      }
      catch (IOException e) {
        e.printStackTrace();
        success = false;
      }
    }

    return success;
  }

  /**
   * A command-line program that finds all sources whose sensor data are not monotonically
   * increasing for a given time period and then returns a string of those sources along with
   * timestamps for those data.
   * 
   * @param args None.
   */
  public static void main(String[] args) {
    HireapDataValidator validator = new HireapDataValidator();
    if (!validator.setup()) {
      System.exit(1);
    }

    try {
      XMLGregorianCalendar startTime = Tstamp.makeTimestamp("2010-01-01T06:00:00.000-10:00");
      XMLGregorianCalendar endTime = Tstamp.makeTimestamp("2011-04-01T06:00:00.000-10:00");

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
