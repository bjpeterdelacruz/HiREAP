package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export HNEI energy data for one or more sources at a given sampling interval over
 * a period of time to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EnergyExporter extends HneiExporter {

  /** Total number of sources. */
  protected int numSources;

  /** Sampling interval in minutes for energy consumed. */
  protected int samplingInterval;

  /**
   * Creates a new EnergyExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public EnergyExporter(WattDepotClient client) {
    super(client);
    this.numSources = 0;
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
      this.sourceNames.add(command);
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
   * Gets the energy consumed for each source that the user inputed at the specified sampling
   * interval (in minutes).
   * 
   * @return Data to output to CSV file.
   */
  public String getEnergyData() {
    double energy = 0.0;
    StringBuffer buffer = new StringBuffer();
    String msg = "Timestamp";

    for (String s : this.sourceNames) {
      buffer.append(msg);
      msg = "," + s;
    }
    buffer.append(msg);

    XMLGregorianCalendar start = this.startTimestamp;
    XMLGregorianCalendar end = Tstamp.incrementMinutes(start, this.samplingInterval);
    while (Tstamp.lessThan(start, this.endTimestamp)) {
      msg = "\n" + end + ",";
      buffer.append(msg);
      try {
        for (String s : this.sourceNames) {
          try {
            end = Tstamp.incrementMinutes(start, this.samplingInterval);
            energy += this.client.getEnergyConsumed(s, start, end, this.samplingInterval);
            msg = String.format("%.0f", energy) + ",";
          }
          catch (BadXmlException e) {
            msg = "N/A,";
          }
          buffer.append(msg);
        }
        buffer.deleteCharAt(buffer.length() - 1);
      }
      catch (WattDepotClientException e) {
        e.printStackTrace();
        return null;
      }
      start = Tstamp.incrementMinutes(start, this.samplingInterval);
      end = Tstamp.incrementMinutes(end, this.samplingInterval);
    }
    return buffer.toString();
  }

  /**
   * Prints information in SensorData objects to a CSV file.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean printData() {
    String today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");
    System.out.println("Generating CSV file...\n");
    System.out.println("Output file: " + today + ".csv\n");

    File outputFile = new File(today + ".csv");
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    boolean success = true;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));

      String result = this.getEnergyData();
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
   * Command-line program that will generate a CSV file containing energy information for one or
   * more sources over a given time period and at a given sampling interval.
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
    EnergyExporter output = new EnergyExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getNumSources(br) || !output.getSourceNames(br) || !output.getDates(br)) {
      System.exit(1);
    }

    if (!output.getSamplingInterval(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
