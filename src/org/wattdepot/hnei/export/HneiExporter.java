package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * This class will export HNEI data for one or more sources to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiExporter extends Exporter {

  /**
   * Creates a new HneiExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public HneiExporter(WattDepotClient client) {
    this.client = client;
    this.sensorDatas = new ArrayList<SensorData>();
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.sources = null;
    this.sourceNames = new ArrayList<String>();
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    String str = null;
    if (this.printVerbose) {
      str = "Account,Install Date,MTU ID,Port,Meter Type,Raw Reading,Reading,Reading Date,RSSI,";
      str += "Monotonically Increasing?,Hourly or Daily?\n";
    }
    else {
      str = "Account,MTU ID,Reading,Reading Date,Monotonically Increasing?,Hourly or Daily?\n";
    }
    return str;
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

    if (this.printVerbose) {
      // Print everything stored in SensorData object.
      buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
      str = "," + datum.getProperty("installDate") + "," + datum.getProperty("mtuID");
      buffer.append(str);
      str = "," + datum.getProperty("port") + "," + datum.getProperty("meterType");
      buffer.append(str);
      str = "," + Integer.parseInt(datum.getProperty("rawRead"));
      buffer.append(str);
      str = "," + datum.getProperty(SensorData.POWER_CONSUMED) + "," + datum.getTimestamp();
      buffer.append(str);
      str = "," + datum.getProperty("rssi");
      buffer.append(str);
      if (datum.getProperty("isMonotonicallyIncreasing").equals("true")) {
        str = ",yes";
      }
      else {
        str = ",no";
      }
      buffer.append(str);
      if (datum.getProperty("daily") == null) {
        str = ",hourly";
      }
      else {
        str = ",daily";
      }
      buffer.append(str);
    }
    else {
      // Only print source, MTU ID, power consumed, yes/no if data is/is not monotonically
      // increasing, and hourly/daily.
      buffer.append(datum.getSource().substring(datum.getSource().lastIndexOf("/") + 1));
      str = "," + datum.getProperty("mtuID");
      buffer.append(str);
      str = "," + datum.getProperty(SensorData.POWER_CONSUMED) + "," + datum.getTimestamp();
      buffer.append(str);
      if (datum.getProperty("isMonotonicallyIncreasing").equals("true")) {
        str = ",yes";
      }
      else {
        str = ",no";
      }
      buffer.append(str);
      if (datum.getProperty("daily") == null) {
        str = ",hourly";
      }
      else {
        str = ",daily";
      }
      buffer.append(str);
    }
    str = "\n";
    buffer.append(str);

    return buffer.toString();
  }

  /**
   * Command-line program that will generate a CSV file containing sensor data for all sources for
   * the given time period.
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
    HneiExporter output = new HneiExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.getVerboseOption(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
