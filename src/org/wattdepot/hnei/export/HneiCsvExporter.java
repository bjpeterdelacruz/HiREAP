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
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export HNEI data to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvExporter {

  /** Total number of sources. */
  protected int numSources;

  /** List of source names. */
  protected List<String> sourceNames;

  /** Timestamp used to get first sensor data. */
  protected XMLGregorianCalendar startTimestamp;

  /** Timestamp used to get last sensor data. */
  protected XMLGregorianCalendar endTimestamp;

  /** Formats dates that are in the format MM/DD/YYYY. */
  protected SimpleDateFormat formatDate;

  /** Sampling interval in minutes for energy consumed. */
  protected int samplingInterval;

  /**
   * Creates a new HneiCsvExporter object.
   */
  public HneiCsvExporter() {
    this.numSources = 0;
    this.sourceNames = new ArrayList<String>();
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.samplingInterval = 0;

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

    for (int i = 0; i < numSources; i++) {
      System.out.print("Please enter the name of source " + (i + 1) + ": ");
      try {
        if ((command = br.readLine()) == null) {
          return false;
        }
      }
      catch (IOException e) {
        e.printStackTrace();
        return false;
      }
      this.sourceNames.add(command);
    }
    return true;
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
        return false;
      }
      this.startTimestamp = Tstamp.makeTimestamp(this.formatDate.parse(command).getTime());
      System.out.print("Please enter an end date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
      if ((command = br.readLine()) == null) {
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
   * Gets the energy consumed for each source that the user inputed at the specified sampling
   * interval (in minutes).
   * 
   * @param client Used to get data from the WattDepot server.
   * @return Data to output to CSV file.
   */
  public String getSensorDatas(WattDepotClient client) {
    double energy = 0;
    StringBuffer buffer = new StringBuffer();
    String msg = "Timestamp";

    for (String s : this.sourceNames) {
      buffer.append(msg);
      msg = "," + s;
    }
    buffer.append(msg);

    XMLGregorianCalendar start = this.startTimestamp;
    XMLGregorianCalendar end = null;
    while (Tstamp.lessThan(start, this.endTimestamp)) {
      msg = "\n" + start + ",";
      buffer.append(msg);
      try {
        for (String s : this.sourceNames) {
          try {
            end = Tstamp.incrementMinutes(start, this.samplingInterval);
            energy = client.getEnergyConsumed(s, start, end, this.samplingInterval);
            msg = String.format("%.2f", energy) + ",";
            buffer.append(msg);
          }
          catch (BadXmlException e) {
            msg = "N/A,";
            buffer.append(msg);
          }
        }
        buffer.deleteCharAt(buffer.length() - 1);
      }
      catch (WattDepotClientException e) {
        e.printStackTrace();
        return null;
      }
      start = Tstamp.incrementMinutes(start, this.samplingInterval);
    }
    return buffer.toString();
  }

  /**
   * Command-line program to generate CSV file containing information about one or more sources at a
   * particular timestamp, which is inputed by the user.
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

    HneiCsvExporter output = new HneiCsvExporter();
    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".\n");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getNumSources(br) || !output.getSourceNames(br) || !output.getDates(br)) {
      System.exit(1);
    }

    if (!output.getSamplingInterval(br)) {
      System.exit(1);
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
      System.exit(1);
    }

    System.out.println("\nGenerating CSV file...\n");
    System.out.println("Output file: " + today + ".csv\n");
    String result = output.getSensorDatas(client);
    System.out.println(result);
    try {
      writer.write(result);
      writer.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

  }

}
