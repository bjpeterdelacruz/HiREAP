package org.wattdepot.hnei.csvexport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * A command-line client for displaying information provided by HNEI that is stored on the WattDepot
 * server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiExporter {

  /** Class to handle grabbing daily data. */
  private DailySensorData dailyData;

  /** Class to handle grabbing hourly data. */
  private HourlySensorData hourlyData;

  /**
   * Creates a new HneiExporter object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public HneiExporter(WattDepotClient client) {
    this.dailyData = new DailySensorData(client);
    this.hourlyData = new HourlySensorData(client);
  }

  /**
   * Provides useful information on how to use this client.
   * 
   * @return A help message.
   */
  public String getHelp() {
    String msg = "\n**********\n";
    msg += "To use this program, type one of the following commands:\n\n";
    msg += ">> dailysensordata [source] [day]\nRetrieves data for a source at the given day ";
    msg += " (hh/DD/yyyy, e.g. 1/20/2011).\n\n";
    msg += "**********\n";
    return msg;
  }

  /**
   * The command-line client that polls information from the WattDepot server.
   * 
   * @param args URI, username, and password to connect to WattDepot server.
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
    if (!client.isHealthy() || !client.isAuthenticated()) {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }
    System.out.println("Successfully connected to " + client.getWattDepotUri() + ".");

    HneiExporter hneiExporter = new HneiExporter(client);
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    String line = null;
    String[] command = null;
    while (true) {
      System.out.print("Enter a command (type \"q\" to quit or \"h\" for help): ");
      try {
        if ((line = br.readLine()) == null) {
          System.err.println("There was a problem reading in your command.");
          System.exit(1);
        }
        command = line.split("\\s");
      }
      catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }

      if (command[0].equals("q") || command[0].equals("quit")) {
        break;
      }
      else if (command[0].equals("h") || command[0].equals("help")) {
        System.out.println(hneiExporter.getHelp());
      }
      else if (command[0].equals("dailysensordata") && command.length == 3) {
        SensorData datum = hneiExporter.dailyData.getSensorData(command[1], command[2]);
        if (datum == null) {
          System.out
              .println("No data for " + command[2] + " exists for source " + command[1] + ".");
        }
        else {
          System.out.println(datum);
        }
      }
      else if (command[0].equals("hourlysensordata") && command.length == 3) {
        List<SensorData> data = hneiExporter.hourlyData.getSensorDatas(command[1], command[2]);
        if (data.isEmpty()) {
          System.out
              .println("No data for " + command[2] + " exists for source " + command[1] + ".");
        }
        else {
          for (SensorData d : data) {
            System.out.println(d);
          }
        }
      }
      else {
        System.out
            .println("Invalid command. Please try again (type \"h\" for available commands).");
      }
    }
    System.out.println("Exiting...");
  }
}
