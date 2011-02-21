package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
   * @param datum SensorData object from which to extract information.
   * @return Information stored in the SensorData object.
   */
  @Override
  public String getInfo(SensorData datum) {
    String str = null;
    StringBuffer buffer = new StringBuffer();
    buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
    str = "," + datum.getTimestamp().toString();
    buffer.append(str);
    str = "," + datum.getProperty(SensorData.POWER_CONSUMED);
    buffer.append(str);
    str = "," + datum.getProperty("airConditioner") + "," + datum.getProperty("waterHeater");
    buffer.append(str);
    str = "," + datum.getProperty("dryer") + "\n";
    buffer.append(str);
    return buffer.toString();
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

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
