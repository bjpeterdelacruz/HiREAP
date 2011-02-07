package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export HNEI data to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvExporter {

  /** Used to grab data from the WattDepot server. */
  protected WattDepotClient client;

  /** List of all sources on the WattDepot server. */
  protected List<SensorData> sensorDatas;

  /** Timestamp used to get first sensor data. */
  protected XMLGregorianCalendar startTimestamp;

  /** Timestamp used to get last sensor data. */
  protected XMLGregorianCalendar endTimestamp;

  /** Formats dates that are in the format MM/DD/YYYY. */
  protected SimpleDateFormat formatDate;

  /** List of all sources that are stored on WattDepot server. */
  protected List<Source> sources;

  /** List of source names. */
  protected List<String> sourceNames;

  /**
   * Creates a new HneiCsvExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public HneiCsvExporter(WattDepotClient client) {
    this.client = client;
    this.sensorDatas = new ArrayList<SensorData>();
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.sources = null;
    this.sourceNames = new ArrayList<String>();
  }

  /**
   * Gets a date from the user via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getDates(BufferedReader br) {
    System.out.print("Please enter a start date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
    String command = null;

    try {
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in start timestamp.");
        return false;
      }
      this.startTimestamp = Tstamp.makeTimestamp(this.formatDate.parse(command).getTime());
      System.out.print("Please enter an end date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in end timestamp.");
        return false;
      }
      XMLGregorianCalendar timestamp =
          Tstamp.makeTimestamp(this.formatDate.parse(command).getTime());
      this.endTimestamp = Tstamp.incrementSeconds(Tstamp.incrementDays(timestamp, 1), -1);
    }
    catch (ParseException e) {
      e.printStackTrace();
      return false;
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Gets sensor data for all sources for the given time period.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean getData() {
    try {
      this.sources = client.getSources();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    try {
      List<SensorData> data = null;
      for (Source s : this.sources) {
        data = client.getSensorDatas(s.getName(), this.startTimestamp, this.endTimestamp);
        if (!data.isEmpty()) {
          for (SensorData datum : data) {
            this.sensorDatas.add(datum);
            this.sourceNames.add(s.getName());
          }
        }
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Command-line program that will generate a CSV file containing sensor data for all sources for
   * the given time period.
   * 
   * @param args Server URI, username, and password to connect to the WattDepot server.
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
    HneiCsvExporter output = new HneiCsvExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.getData()) {
      System.exit(1);
    }

    Collections.sort(output.sensorDatas, new SensorDataSorter("source"));
    // Collections.sort(output.sensorDatas, new SensorDataSorter("mtuID"));

    String source = "";
    for (SensorData datum : output.sensorDatas) {
      source = datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1);
      System.out.println(source + "," + datum.getProperty("mtuID"));
    }

  }

}
