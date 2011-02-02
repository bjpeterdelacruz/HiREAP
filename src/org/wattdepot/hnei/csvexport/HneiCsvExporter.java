package org.wattdepot.hnei.csvexport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.wattdepot.client.WattDepotClient;

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

  /**
   * Creates a new HneiCsvExporter object.
   */
  public HneiCsvExporter() {
    this.numSources = 0;
    this.sourceNames = new ArrayList<String>();

  }

  /**
   * Gets the number of sources user wants information about via the command-line.
   * 
   * @param br Used to get information from the command-line.
   * @return True if input is successful, false otherwise.
   */
  public boolean getNumSources(BufferedReader br) {
    System.out.print("Enter the number of sources for which you want data: ");
    String command = null;
    try {
      command = br.readLine();
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
    List<String> sources = new ArrayList<String>();
    String command = null;
    for (int i = 0; i < numSources; i++) {
      System.out.print("Please enter the name of source " + (i + 1) + ": ");
      try {
        command = br.readLine();
      }
      catch (IOException e) {
        e.printStackTrace();
        return false;
      }
      sources.add(command);
    }
    return true;
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

    // Grab data from CSV file.
    HneiCsvExporter output = new HneiCsvExporter();
    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getNumSources(br) || !output.getSourceNames(br)) {
      System.exit(1);
    }

    System.out.print("Please enter a date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
  }

}
