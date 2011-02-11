package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * This class will export HNEI TED data for a source over a given period of time to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TedExporter extends HneiExporter {

  /**
   * Creates a new TedExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public TedExporter(WattDepotClient client) {
    super(client);
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    return "Source,Timestamp,MTU1,MTU2,MTU3,MTU4,Other,AC Off?,Blank,Not Blank\n";
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
    str = "," + datum.getProperty("mtu2") + "," + datum.getProperty("mtu3");
    buffer.append(str);
    str = "," + datum.getProperty("mtu4") + "," + datum.getProperty("other");
    buffer.append(str);
    str = "," + getYesOrNo(datum.getProperty("isAirConditionerOff"));
    buffer.append(str);
    str = "," + datum.getProperty("blank") + "," + datum.getProperty("not blank") + "\n";
    buffer.append(str);
    return buffer.toString();
  }

  /**
   * Returns "Yes" if "true" or "No" if "false".
   * 
   * @param str "true" or "false".
   * @return "Yes" or "No".
   */
  private String getYesOrNo(String str) {
    if ("true".equals(str)) {
      return "Yes";
    }
    else {
      return "No";
    }
  }

  /**
   * Command-line program that will generate a CSV file containing TED data for a source
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
    TedExporter output = new TedExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (!output.getDates(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
