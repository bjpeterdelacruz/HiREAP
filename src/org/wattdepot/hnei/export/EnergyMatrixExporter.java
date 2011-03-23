package org.wattdepot.hnei.export;

// import java.io.BufferedWriter;
// import java.io.File;
// import java.io.FileWriter;
// import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This program will output a matrix containing energy consumed data in kWh to a CSV file. The rows
 * represent timestamps, and the columns represent sources.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EnergyMatrixExporter {

  /** Used to connect to WattDepot server. */
  protected WattDepotClient client;

  /** Formats dates that are in the format <code>yyyy-MM-dd hh:mm:ss a</code>. */
  protected SimpleDateFormat formatDateTime;

  /**
   * Creates a new EnergyMatrixExporter object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public EnergyMatrixExporter(WattDepotClient client) {
    this.client = client;
    this.formatDateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a", Locale.US);
  }

  /**
   * Returns a list of sensor data for a source between two timestamps.
   * 
   * @param sourceName Name of a source.
   * @param prevTimestamp Previous timestamp.
   * @param currTimestamp Current timestamp.
   * @return A list of sensor data for a source between two timestamps.
   */
  public List<SensorData> getSensorDatas(String sourceName, XMLGregorianCalendar prevTimestamp,
      XMLGregorianCalendar currTimestamp) {
    try {
      return this.client.getSensorDatas(sourceName, prevTimestamp, currTimestamp);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns the energy consumed given a range.
   * 
   * @param sourceName Name of a source.
   * @param previousData Previous energy data for a source.
   * @param currentData Current energy data for a source.
   * @return A double representing the energy consumed for a source.
   */
  public double getEnergy(String sourceName, SensorData previousData, SensorData currentData) {
    double currReading = currentData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    double prevReading = previousData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    return currReading - prevReading;
  }

  /**
   * This program will output a matrix containing energy consumed data in kWh to a CSV file. The
   * rows represent timestamps, and the columns represent sources.
   * 
   * @param args Contains filename, server URI, username, and password.
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
    EnergyMatrixExporter exporter = new EnergyMatrixExporter(client);

    /*
     * File outputFile = new File("output.csv"); outputFile.setWritable(true); BufferedWriter writer
     * = null; try { writer = new BufferedWriter(new FileWriter(outputFile)); } catch (IOException
     * e) { e.printStackTrace(); System.exit(1); }
     */

    List<Source> sources = null;
    try {
      sources = client.getSources();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      System.exit(1);
    }

    XMLGregorianCalendar start = null;
    XMLGregorianCalendar end = null;
    try {
      start =
          Tstamp.makeTimestamp(exporter.formatDateTime.parse("2011-02-07 8:00:00 AM").getTime());
      end = Tstamp.makeTimestamp(exporter.formatDateTime.parse("2011-02-08 8:00:00 AM").getTime());
    }
    catch (ParseException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Double energy = null;
    List<Double> energyDatas = new ArrayList<Double>();
    List<SensorData> sensorDatas = null;

    for (Source s : sources) {
      sensorDatas = exporter.getSensorDatas(s.getName(), start, end);
      for (int currIdx = 1, prevPos = 0; currIdx < sensorDatas.size(); currIdx++, prevPos++) {
        if (sensorDatas.size() > 2) {
          System.out.print(sensorDatas.get(currIdx).getTimestamp() + ": ");
          energy =
              exporter.getEnergy(s.getName(), sensorDatas.get(prevPos),
                  sensorDatas.get(currIdx));
          System.out.println(String.format("%.5f", (energy / 1000)) + " kWh");
        }
        else {
          energyDatas.add(new Double(0.0));
          break;
        }
      }
    }
  }

}
