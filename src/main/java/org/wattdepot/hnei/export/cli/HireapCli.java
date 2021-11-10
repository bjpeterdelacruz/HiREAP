package org.wattdepot.hnei.export.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.hnei.export.SourceComparator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * A command-line client for displaying information provided by HNEI that is stored on the WattDepot
 * server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HireapCli {

  /** A list of commands that will grab data from the WattDepot server. */
  private Map<String, Retriever> commands;

  /**
   * Creates a new HneiExporter object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public HireapCli(WattDepotClient client) {
    this.commands = new HashMap<String, Retriever>();
    this.commands.put("all_data", new SourceSensorDatas(client));
    this.commands.put("interpolate", new InterpolatedSensorData(client));
  }

  /**
   * Provides useful information on how to use this client.
   */
  public void getHelp() {
    Iterator<Entry<String, Retriever>> i = this.commands.entrySet().iterator();
    System.out.println("\n**********\n");
    System.out.println("To use this program, type one of the following commands:\n");
    System.out.println(">> q | quit\nQuits the program.\n");
    System.out.println(">> h | help\nDisplays this help message.\n");
    System.out
        .println(">> (p | properties) [source]\nDisplays the properties for a given source.\n");
    System.out.println(">> a | all_sources");
    System.out.println("Displays all sources that are available on the WattDepot server.\n");
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

    HireapCli hneiExporter = new HireapCli(client);
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

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

      if (command[0].equalsIgnoreCase("q") || command[0].equalsIgnoreCase("quit")) {
        break;
      }
      else if (command[0].equals("h") || command[0].equals("help")) {
        hneiExporter.getHelp();
      }
      else if (command[0].equalsIgnoreCase("all_data") && command.length == 4) {
        String a = "all_data";
        if (!hneiExporter.commands.get(a).getSensorData(command[1], command[2], command[3],
            command[3])) {
          System.exit(1);
        }
      }
      else if (command[0].equalsIgnoreCase("interpolate") && command.length == 4) {
        String a = "interpolate";
        if (!hneiExporter.commands.get(a).getSensorData(command[1], command[2], command[3],
            command[3])) {
          System.exit(1);
        }
      }
      else if ((command[0].equalsIgnoreCase("properties") || command[0].equalsIgnoreCase("p"))
          && command.length == 2) {
        try {
          List<Property> properties = client.getSource(command[1]).getProperties().getProperty();
          for (Property p : properties) {
            System.out.println(p.getKey() + ": " + p.getValue());
          }
        }
        catch (ResourceNotFoundException e) {
          System.out.println("Source not found. Please try again.");
        }
        catch (WattDepotClientException e) {
          System.out.println(e);
          break;
        }
      }
      else if ((command[0].equalsIgnoreCase("all_sources") || command[0].equalsIgnoreCase("a"))
          && command.length == 1) {
        try {
          List<Source> sources = client.getSources();
          Collections.sort(sources, new SourceComparator());
          for (Source s : sources) {
            System.out.print(s.getName() + " :");
            for (Property p : s.getProperties().getProperty()) {
              System.out.print(" [[key : " + p.getKey() + "] [value : " + p.getValue() + "]]");
            }
            System.out.println();
          }
        }
        catch (WattDepotClientException e) {
          System.out.println(e);
          break;
        }
      }
      else {
        String msg = "Invalid command. Please try again (type \"h\" for available commands).";
        System.out.println(msg);
      }
    }

    System.out.println("Exiting...");
  }
}
