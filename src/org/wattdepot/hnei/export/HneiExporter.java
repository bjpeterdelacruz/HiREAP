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

  /** Field to sort data by. */
  private String sortField;

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
      System.out.println("Fetching data from server... Please wait.");
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
   * Prints information in SensorData objects to a CSV file.
   * 
   * @param writer CSV file to write data to.
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean printFields(BufferedWriter writer) {
    String str = "";
    StringBuffer buffer = new StringBuffer();

    if (this.printVerbose) {
      str = "Account,Install Date,MTU ID,Port,Meter Type,Raw Reading,Reading,Reading Date,RSSI,";
      str += "Monotonically Increasing?,Hourly or Daily?\n";
      buffer.append(str);
    }
    else {
      str = "Account,MTU ID,Reading,Reading Date,Monotonically Increasing?,Hourly or Daily?\n";
      buffer.append(str);
    }

    for (SensorData datum : this.sensorDatas) {
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
    }

    try {
      writer.write(buffer.toString());
      System.out.println(buffer.toString());
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Gets the option (1-9) to sort data by from the user via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return String representing option if successful, null otherwise.
   */
  protected String getUserInput(BufferedReader br) {
    String msg = "Enter number for field to sort data by (enter \"F\" to view list of options): ";
    String command = null;
    String error = "Error encountered while trying to read in option for verbose output.";

    System.out.print(msg);
    try {
      if ((command = br.readLine()) == null) {
        System.out.println(error);
        return null;
      }
      while (command.equalsIgnoreCase("F")) {
        System.out.println("1 -- Account");
        System.out.println("2 -- MTU ID");
        System.out.println("3 -- Reading");
        System.out.println("4 -- Reading Date");
        System.out.println("5 -- Monotonically Increasing");
        System.out.println("6 -- Hourly");
        System.out.println("7 -- Daily");
        System.out.print(msg);
        if ((command = br.readLine()) == null) {
          System.out.println(error);
          return null;
        }
      }
    }
    catch (IOException e) {
      System.out.println(error);
      return null;
    }

    return command;
  }

  /**
   * Returns the field to sort data by.
   * 
   * @return Field to sort data by.
   */
  public String getSortField() {
    return this.sortField;
  }

  /**
   * Gets the field to sort data by from the user via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getSortOption(BufferedReader br) {
    String command = null;
    if ((command = this.getUserInput(br)) == null) {
      return false;
    }

    int choice = 0;
    boolean isValidInput = false;
    while (!isValidInput) {
      try {
        choice = Integer.parseInt(command);
        if (choice < 1 || choice > 7) {
          throw new NumberFormatException();
        }
        isValidInput = true;
      }
      catch (NumberFormatException e) {
        System.out.println("Invalid option. Please try again.");
        if ((command = this.getUserInput(br)) == null) {
          return false;
        }
        isValidInput = false;
      }
    }

    switch (choice) {
    case 1:
      this.sortField = "account";
      break;
    case 2:
      this.sortField = "mtuID";
      break;
    case 3:
      this.sortField = "reading";
      break;
    case 4:
      this.sortField = "timestamp";
      break;
    case 5:
      this.sortField = "isMonotonicallyIncreasing";
      break;
    case 6:
      this.sortField = "hourly";
      break;
    case 7:
      this.sortField = "daily";
      break;
    default:
      System.out.println("Option not specified. Exiting...");
      this.sortField = null;
      break;
    }

    return true;
  }

  /**
   * Creates CSV file and prints its contents to the screen.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean printDatas() {
    String today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");
    System.out.println("Generating CSV file...\n");
    System.out.println("Output file: " + today + ".csv\n");

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

    if (!this.printFields(writer)) {
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

    if (!output.getDates(br) || !output.getData() || !output.getVerboseOption(br)) {
      System.exit(1);
    }

    if (!output.getSortOption(br) || output.getSortField() == null) {
      System.exit(1);
    }

    Collections.sort(output.getSensorDatas(), new SensorDataSorter(output.getSortField()));

    output.printDatas();

  }

}
