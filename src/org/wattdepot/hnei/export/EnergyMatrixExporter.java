package org.wattdepot.hnei.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This program will output a matrix containing energy consumed data in kWh to a CSV file. The rows
 * represent timestamps, and the columns represent sources.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EnergyMatrixExporter {

  /** Used to connect to WattDepot server. */
  protected WattDepotClient client;

  /** Formats dates that are in the format <code>yyyy-MM-dd hh:mm:ss a</code>. */
  protected SimpleDateFormat formatDateTime;

  /**
   * Creates a new EnergyMatrixExporter object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public EnergyMatrixExporter(WattDepotClient client) {
    this.client = client;
    this.formatDateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a", Locale.US);
  }

  /**
   * Returns a list of sensor data for a source between two timestamps.
   * 
   * @param sourceName Name of a source.
   * @param prevTimestamp Previous timestamp.
   * @param currTimestamp Current timestamp.
   * @return A list of sensor data for a source between two timestamps.
   */
  public List<SensorData> getSensorDatas(String sourceName, XMLGregorianCalendar prevTimestamp,
      XMLGregorianCalendar currTimestamp) {
    try {
      return this.client.getSensorDatas(sourceName, prevTimestamp, currTimestamp);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns the energy consumed given a range.
   * 
   * @param sourceName Name of a source.
   * @param previousTimestamp Previous timestamp at which to grab energy data.
   * @param currentTimestamp Current timestamp at which to grab energy data.
   * @param samplingInterval Sampling interval in minutes.
   * @return A double representing the energy consumed for a source.
   */
  public double getEnergy(String sourceName, XMLGregorianCalendar previousTimestamp,
      XMLGregorianCalendar currentTimestamp, int samplingInterval) {
    try {
      return this.client.getEnergyConsumed(sourceName, previousTimestamp, currentTimestamp,
          samplingInterval);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return Double.NaN;
    }
  }

  /**
   * Returns the time given in milliseconds in string format.
   * 
   * @param milliseconds Time in milliseconds.
   * @return Time in string format <code>yyyy-MM-dd hh:mm:ss</code>.
   */
  public String getTimestamp(long milliseconds) {
    return this.formatDateTime.format(milliseconds);
  }

  /**
   * This program will output a matrix containing energy consumed data in kWh to a CSV file. The
   * rows represent timestamps, and the columns represent sources.
   * 
   * @param args Contains filename, server URI, username, and password.
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    String serverUri = args[0];
    String username = args[1];
    String password = args[2];

    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".\n");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }
    EnergyMatrixExporter exporter = new EnergyMatrixExporter(client);

    // Create output file.
    File outputFile = new File("output.csv");
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    List<Source> sources = null;
    try {
      sources = client.getSources();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Date date = null;
    XMLGregorianCalendar start = null;
    XMLGregorianCalendar end = null;
    try {
      date = exporter.formatDateTime.parse("2011-02-07 8:00:00 AM");
      start = Tstamp.makeTimestamp(date.getTime());
      end = Tstamp.makeTimestamp(exporter.formatDateTime.parse("2011-02-08 8:00:00 AM").getTime());
    }
    catch (ParseException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Writer header to output file.
    try {
      writer.write("Timestamp");
      for (Source s : sources) {
        writer.write("," + s.getName());
      }
      writer.write("\n");
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    double energy = 0.0;
    date = start.toGregorianCalendar().getTime();
    String msg = "";
    StringBuffer buffer = new StringBuffer();
    XMLGregorianCalendar currTstamp = Tstamp.makeTimestamp(date.getTime());
    XMLGregorianCalendar nextTstamp = null;

    // Energy is in kWh.
    try {
      while (Tstamp.lessThan(currTstamp, end)) {
        buffer.append(exporter.getTimestamp(currTstamp.toGregorianCalendar().getTime().getTime()));
        for (Source s : sources) {
          nextTstamp = Tstamp.incrementMinutes(currTstamp, 60);
          energy = exporter.getEnergy(s.getName(), currTstamp, nextTstamp, 60) / 1000.0;
          msg = "," + String.format("%.05f", energy);
          buffer.append(msg);
        }
        writer.write(buffer.toString() + "\n");
        currTstamp = Tstamp.incrementMinutes(currTstamp, 60);
        buffer = new StringBuffer();
      }
      writer.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("Finished writing energy data to output file!");
  }

}
