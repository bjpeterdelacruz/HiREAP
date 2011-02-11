package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export HNEI data for one or more sources to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiExporter implements Exportable {

  /** Used to grab data from the WattDepot server. */
  protected WattDepotClient client;

  /** List of all sources on the WattDepot server. */
  private List<SensorData> sensorDatas;

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
   * Option to print only the most relevant information or all information in SensorData objects.
   */
  protected boolean printVerbose;

  /**
   * Creates a new HneiExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public HneiExporter(WattDepotClient client) {
    this.client = client;
    this.sensorDatas = new ArrayList<SensorData>();
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.sources = null;
    this.sourceNames = new ArrayList<String>();
  }

  /**
   * Returns a list of SensorData objects.
   * 
   * @return List of SensorData objects.
   */
  public List<SensorData> getSensorDatas() {
    return this.sensorDatas;
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
   * Asks the user via the command-line whether to print everything stored in SensorData object.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getVerboseOption(BufferedReader br) {
    System.out.print("Verbose output [yes|no]? ");
    String command = null;

    try {
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in option for verbose output.");
        return false;
      }
      if (command.equalsIgnoreCase("y") || command.equalsIgnoreCase("yes")) {
        this.printVerbose = true;
      }
      else {
        this.printVerbose = false;
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    String str = null;
    if (this.printVerbose) {
      str = "Account,Install Date,MTU ID,Port,Meter Type,Raw Reading,Reading,Reading Date,RSSI,";
      str += "Monotonically Increasing?,Hourly or Daily?\n";
    }
    else {
      str = "Account,MTU ID,Reading,Reading Date,Monotonically Increasing?,Hourly or Daily?\n";
    }
    return str;
  }

  /**
   * Returns information stored in a SensorData object.
   * 
   * @param datum SensorData object from which to extract information.
   * @return Information stored in the SensorData object.
   */
  @Override
  public String getInfo(SensorData datum) {
    String str = null;
    StringBuffer buffer = new StringBuffer();

    if (this.printVerbose) {
      // Print everything stored in SensorData object.
      buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
      str = "," + datum.getProperty("installDate") + "," + datum.getProperty("mtuID");
      buffer.append(str);
      str = "," + datum.getProperty("port") + "," + datum.getProperty("meterType");
      buffer.append(str);
      str = "," + Integer.parseInt(datum.getProperty("rawRead"));
      buffer.append(str);
      str = "," + datum.getProperty(SensorData.POWER_CONSUMED) + "," + datum.getTimestamp();
      buffer.append(str);
      str = "," + datum.getProperty("rssi");
      buffer.append(str);
      if (datum.getProperty("isMonotonicallyIncreasing").equals("true")) {
        str = ",yes";
      }
      else {
        str = ",no";
      }
      buffer.append(str);
      if (datum.getProperty("daily") == null) {
        str = ",hourly";
      }
      else {
        str = ",daily";
      }
      buffer.append(str);
    }
    else {
      // Only print source, MTU ID, power consumed, yes/no if data is/is not monotonically
      // increasing, and hourly/daily.
      buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
      str = "," + datum.getProperty("mtuID");
      buffer.append(str);
      str = "," + datum.getProperty(SensorData.POWER_CONSUMED) + "," + datum.getTimestamp();
      buffer.append(str);
      if (datum.getProperty("isMonotonicallyIncreasing").equals("true")) {
        str = ",yes";
      }
      else {
        str = ",no";
      }
      buffer.append(str);
      if (datum.getProperty("daily") == null) {
        str = ",hourly";
      }
      else {
        str = ",daily";
      }
      buffer.append(str);
    }
    str = "\n";
    buffer.append(str);

    return buffer.toString();
  }

  /**
   * Gets sensor data for all sources for the given time period.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean printData() {
    try {
      this.sources = client.getSources();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    String today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");

    File outputFile = new File(today + ".csv");
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    StringBuffer buffer = new StringBuffer();

    buffer.append(this.getTableHeader());

    try {
      System.out.println("Fetching data from server... Please wait.");
      List<SensorData> data = null;
      for (Source s : this.sources) {
        data = client.getSensorDatas(s.getName(), this.startTimestamp, this.endTimestamp);
        if (!data.isEmpty()) {
          for (SensorData datum : data) {
            buffer.append(this.getInfo(datum));
          }
        }
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    try {
      writer.write(buffer.toString());
      System.out.println(buffer.toString());
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    try {
      writer.close();
    }
    catch (IOException e) {
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
    HneiExporter output = new HneiExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.getVerboseOption(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
