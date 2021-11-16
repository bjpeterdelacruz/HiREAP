package org.wattdepot.hnei.export;

import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Classes that implement this abstract class will generate a CSV file containing data for one or
 * more sources.
 * 
 * @author BJ Peter DeLaCruz
 */
public abstract class Exporter {

  /** Log file for this application. */
  protected Logger log;

  /** Output logging information to a text file. */
  protected FileHandler txtFile;

  /** Name of the application on the command line. */
  protected String toolName;

  /** Used to grab data from the WattDepot server. */
  protected WattDepotClient client;

  /** Timestamp used to get first sensor data. */
  protected XMLGregorianCalendar startTimestamp;

  /** Timestamp used to get last sensor data. */
  protected XMLGregorianCalendar endTimestamp;

  /** Formats dates that are in the format MM/dd/yyyy. */
  protected SimpleDateFormat formatDate;

  /** Formats dates that are in the format yyyy-MM-dd hh:mm:ss a. */
  protected SimpleDateFormat formatDateTime;

  /** List of all sources that are stored on WattDepot server. */
  protected List<Source> sources;

  /** Total number of sources. */
  protected int numSources;

  /** Sampling interval in minutes for energy consumed. */
  protected int samplingInterval;

  /** Specifies the type of data: hourly, daily, weekly, or monthly. */
  protected String sourceDataType;

  /** Gets all data from all sources on the WattDepot server. */
  protected static final String ALL_DATA = "all";

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
      return false;
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
   * Sets up the logger and file handler.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean setupLogger() {
    long timeInMillis = Calendar.getInstance().getTimeInMillis();
    String filename = this.toolName + "-" + timeInMillis + ".log";

    this.log.setLevel(Level.INFO);
    try {
      this.txtFile = new FileHandler(filename);
      this.txtFile.setFormatter(new SimpleFormatter());
    }
    catch (IOException e) {
      System.err.println("Unable to create file handler for logger.");
      return false;
    }
    catch (SecurityException e) {
      return false;
    }

    System.out.println("Log file name: " + filename);
    this.log.addHandler(this.txtFile);
    return true;
  }

  /**
   * Closes the log file.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean closeLogger() {
    try {
      this.txtFile.close();
      return true;
    }
    catch (SecurityException e) {
      return false;
    }
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
    catch (ParseException | IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Gets the type of energy data the user wants: hourly, daily, or all data.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getDataType(BufferedReader br) {
    try {
      this.sourceDataType = "";
      while (!this.sourceDataType.equals(SamplingInterval.DAILY)
          && !this.sourceDataType.equals(SamplingInterval.HOURLY)
          && !this.sourceDataType.equals(ALL_DATA)) {
        System.out.print("Please enter the type of data for the sources you want to retrieve ");
        System.out.print("[daily|hourly|all]: ");
        if ((this.sourceDataType = br.readLine()) == null) {
          System.out.println("Error encountered while trying to read in start timestamp.");
          return false;
        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Gets the number of sources user wants information about via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getNumSources(BufferedReader br) {
    System.out.print("Please enter the number of sources for which you want data: ");
    String command = null;

    try {
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in number of sources.");
        return false;
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    try {
      this.numSources = Integer.parseInt(command);
    }
    catch (NumberFormatException e) {
      System.out.println("Invalid input. Exiting...");
      return false;
    }

    return true;
  }

  /**
   * Gets a list of source names from the user via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getSourceNames(BufferedReader br) {
    String command = null;

    for (int i = 0; i < this.numSources; i++) {
      System.out.print("Please enter the name of source " + (i + 1) + ": ");
      try {
        if ((command = br.readLine()) == null) {
          System.out.println("Error encountered while trying to read in source name.");
          return false;
        }
      }
      catch (IOException e) {
        e.printStackTrace();
        return false;
      }
      // this.sourceNames.add(command);
      try {
        this.sources.add(this.client.getSource(command));
      }
      catch (WattDepotClientException e) {
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  /**
   * Gets a sampling interval in minutes from the user via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getSamplingInterval(BufferedReader br) {
    String command = null;
    boolean isValidInput = false;

    while (!isValidInput) {
      try {
        System.out.print("Please enter a sampling interval in minutes: ");
        if ((command = br.readLine()) == null) {
          System.out.println("Error encountered while trying to read in sampling interval.");
          return false;
        }
        this.samplingInterval = Integer.parseInt(command);
        if (this.samplingInterval < 1) {
          throw new NumberFormatException();
        }
        isValidInput = true;
      }
      catch (NumberFormatException e) {
        System.out.print("Error: Input must be a number and greater than 0. ");
        System.out.println("Please try again.");
      }
      catch (IOException e) {
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  /**
   * Gets sensor data for all sources for the given time period.
   * 
   * @return True if successful, false otherwise.
   */
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
    BufferedWriter writer;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile, StandardCharsets.UTF_8));
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    StringBuilder builder = new StringBuilder();

    builder.append(this.getTableHeader());

    try {
      System.out.println("Fetching data from server... Please wait.");
      List<SensorData> data = null;
      for (Source s : this.sources) {
        data = client.getSensorDatas(s.getName(), this.startTimestamp, this.endTimestamp);
        if (!data.isEmpty()) {
          for (SensorData d : data) {
            builder.append(this.getInfo(d));
          }
        }
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    try {
      writer.write(builder.toString());
      System.out.println(builder.toString());
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
   * Gets all of the sources on the WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean getAllSources() {
    try {
      this.sources = this.client.getSources();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }
    return true;
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
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  public abstract String getTableHeader();

  /**
   * Returns information stored in a SensorData object.
   * 
   * @param data SensorData object from which to extract information.
   * @return Information stored in the SensorData object.
   */
  public abstract String getInfo(SensorData data);

}
