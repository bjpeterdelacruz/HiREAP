package org.wattdepot.hnei.csvexport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.wattdepot.client.WattDepotClient;

/**
 * A command-line client for displaying information provided by HNEI that is stored on the WattDepot
 * server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiWattDepotCli {

  /** A list of commands that will grab data from the WattDepot server. */
  private Map<String, Retriever> commands;

  /**
   * Creates a new HneiExporter object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public HneiWattDepotCli(WattDepotClient client) {
    this.commands = new HashMap<String, Retriever>();
    this.commands.put("daily", new DailySensorData(client));
    this.commands.put("hourly", new HourlySensorData(client));
    this.commands.put("non-mono", new NonmonotonicallyIncreasingData(client));
  }

  /**
   * Provides useful information on how to use this client.
   */
  public void getHelp() {
    Iterator<Entry<String, Retriever>> i = this.commands.entrySet().iterator();
    System.out.println("\n**********\n");
    System.out.println("To use this program, type one of the following commands:\n");
    System.out.println(">> q | quit\nQuits the program.");
    System.out.println(">> h | help\nDisplays this help message.");
    while (i.hasNext()) {
      System.out.print(i.next().getValue().getHelp());
    }
    System.out.println("**********\n");
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

    HneiWattDepotCli hneiExporter = new HneiWattDepotCli(client);
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
        hneiExporter.getHelp();
      }
      else if (command[0].equals("daily") && command.length == 3) {
        hneiExporter.commands.get("daily").getSensorData(command[1], command[2]);
      }
      else if (command[0].equals("hourly") && command.length == 3) {
        hneiExporter.commands.get("hourly").getSensorData(command[1], command[2]);
      }
      else if (command[0].equals("non-mono") && command.length == 3) {
        hneiExporter.commands.get("non-mono").getSensorData(command[1], command[2]);
      }
      else {
        String msg = "Invalid command. Please try again (type \"h\" for available commands).";
        System.out.println(msg);
      }
    }
    System.out.println("Exiting...");
  }
}
