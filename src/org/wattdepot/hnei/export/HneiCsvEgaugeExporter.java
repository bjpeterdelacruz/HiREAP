package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * This class will export HNEI Egauge data for a source over a given period of time to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvEgaugeExporter extends HneiCsvExporter {

  /**
   * Creates a new HneiCsvEgaugeExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public HneiCsvEgaugeExporter(WattDepotClient client) {
    super(client);
  }

  /**
   * Prints information in SensorData objects to a CSV file.
   * 
   * @param writer CSV file to write data to.
   * @return True if successful, false otherwise.
   */
  public boolean printFields(BufferedWriter writer) {
    String str = "Source,Timestamp,Whole House,AC,Water Heater,Dryer\n";
    StringBuffer buffer = new StringBuffer();
    buffer.append(str);

    for (SensorData datum : this.getSensorDatas()) {
      buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
      str = "," + datum.getTimestamp().toString();
      buffer.append(str);
      str = "," + datum.getProperty(SensorData.POWER_CONSUMED);
      buffer.append(str);
      str = "," + datum.getProperty("airConditioner") + "," + datum.getProperty("waterHeater");
      buffer.append(str);
      str = "," + datum.getProperty("dryer") + "\n";
      buffer.append(str);
    }

    try {
      writer.write(buffer.toString());
      System.out.println(buffer.toString());
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
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
    HneiCsvEgaugeExporter output = new HneiCsvEgaugeExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.getData()) {
      System.exit(1);
    }

    Collections.sort(output.getSensorDatas(), new SensorDataSorter("timestamp"));

    output.printDatas();

  }

}
