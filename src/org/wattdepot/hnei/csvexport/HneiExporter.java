package org.wattdepot.hnei.csvexport;

import org.wattdepot.client.WattDepotClient;

/**
 * TODO: Fill out later.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiExporter {

  /**
   * TODO: Fill out later.
   * 
   * @param args URI, username, and password to connect to WattDepot server.
   */
  public static void main(String[] args) {
    if (args.length != 4) {
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
  }
}
