package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * This class will export HNEI Egauge data for a source over a given period of time to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EgaugeExporter extends Exporter {

  /**
   * Creates a new EgaugeExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public EgaugeExporter(WattDepotClient client) {
    this.client = client;
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    return "Source,Timestamp,Whole House,AC,Water Heater,Dryer\n";
  }

  /**
   * Returns information stored in a SensorData object.
   * 
   * @param data SensorData object from which to extract information.
   * @return Information stored in the SensorData object.
   */
  @Override
  public String getInfo(SensorData data) {
    StringBuilder builder = new StringBuilder();
    builder.append(data.getSource().substring(data.getSource().lastIndexOf("/") + 1));
    String str = "," + data.getTimestamp().toString();
    builder.append(str);
    str = "," + data.getProperty(SensorData.POWER_CONSUMED);
    builder.append(str);
    str = "," + data.getProperty("airConditioner") + "," + data.getProperty("waterHeater");
    builder.append(str);
    str = "," + data.getProperty("dryer") + "\n";
    builder.append(str);
    return builder.toString();
  }

  /**
   * Command-line program that will generate a CSV file containing Egauge data for a source
   * over a period of time.
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
    EgaugeExporter output = new EgaugeExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    if (!output.getDates(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
