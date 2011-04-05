package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export energy consumed data for one or more sources to a CSV file. The output
 * file is a matrix in which the rows represent sources, the columns represent timestamps, and the
 * cells represent the amount of energy consumed in kWh for a specific time period.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EnergyMatrixExporter extends Exporter {

  /** Header row for matrix. */
  protected List<String> header;

  /**
   * Creates a new EnergyMatrixExporter object.
   * 
   * @param client Used to grab data from the WattDepot server.
   */
  public EnergyMatrixExporter(WattDepotClient client) {
    this.client = client;
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.sources = null;
    this.sourceNames = new ArrayList<String>();
    this.numSources = 0;
    this.samplingInterval = 0;
    this.formatDateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a", Locale.US);
    this.header = new ArrayList<String>();
    this.sources = new ArrayList<Source>();
  }

  /**
   * Gets the energy consumed for each source between startTimestamp and endTimestamp; data is
   * sampled at the specified sampling interval (in minutes).
   * 
   * @return Data to output to CSV file.
   */
  public String getEnergyData() {
    String msg = "";
    StringBuffer buffer = new StringBuffer();
    XMLGregorianCalendar start = this.startTimestamp;
    XMLGregorianCalendar end = Tstamp.incrementMinutes(start, this.samplingInterval);

    // msg = "Number of sources: " + this.sources.size() + "\n\n";
    // msg += "The sampling interval is " + this.samplingInterval + " minute(s).\n\n";
    // msg += "Start: ";
    // msg += this.getTimestamp(this.startTimestamp.toGregorianCalendar().getTime().getTime()) +
    // "\n";
    // msg += "End:   ";
    // msg += this.getTimestamp(this.endTimestamp.toGregorianCalendar().getTime().getTime()) +
    // "\n\n";
    // buffer.append(msg);

    while (Tstamp.lessThan(start, this.endTimestamp)) {
      this.header.add(this.getTimestamp(end.toGregorianCalendar().getTime().getTime()));
      start = Tstamp.incrementMinutes(start, this.samplingInterval);
      end = Tstamp.incrementMinutes(end, this.samplingInterval);
    }
    this.header.add(this.getTimestamp(this.endTimestamp.toGregorianCalendar().getTime().getTime()));
    buffer.append(this.getTableHeader());

    start = this.startTimestamp;
    end = Tstamp.incrementMinutes(start, this.samplingInterval);
    try {
      int count = 1;
      SensorData data = null;
      for (Source s : this.sources) {
        msg = "\n" + s.getName();
        buffer.append(msg);
        while (Tstamp.lessThan(start, this.endTimestamp)) {
          try {
            data = this.client.getEnergy(s.getName(), start, end, this.samplingInterval);
            msg = "," + this.getInfo(data);
          }
          catch (BadXmlException e) {
            msg = ",N/A";
          }
          buffer.append(msg);

          start = Tstamp.incrementMinutes(start, this.samplingInterval);
          end = Tstamp.incrementMinutes(end, this.samplingInterval);
        }
        System.out.print("Finished processing " + count + " out of ");
        System.out.println(this.sources.size() + "...");
        start = this.startTimestamp;
        end = Tstamp.incrementMinutes(start, this.samplingInterval);
        count++;
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }

    return buffer.toString();
  }

  /**
   * Prints energy information to a CSV file.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean printData() {
    String today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");
    System.out.println("Generating CSV file...\n");
    System.out.println("Output file: " + today + ".csv\n");

    File outputFile = new File(today + ".csv");
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    boolean success = true;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));

      String result = this.getEnergyData();
      if (result == null) {
        throw new IOException();
      }

      writer.write(result);
      System.out.println(result);
    }
    catch (IOException e) {
      e.printStackTrace();
      success = false;
    }
    finally {
      try {
        writer.close();
      }
      catch (IOException e) {
        e.printStackTrace();
        success = false;
      }
    }

    return success;
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    StringBuffer buffer = new StringBuffer();
    String msg = "Sources";
    for (String s : this.header) {
      buffer.append(msg);
      msg = "," + s;
    }
    return buffer.toString();
  }

  /**
   * Returns the amount of energy consumed from the SensorData object in kWh.
   * 
   * @param data SensorData object that contains the amount of energy consumed.
   * @return Energy in kWh.
   */
  @Override
  public String getInfo(SensorData data) {
    double energy = data.getPropertyAsDouble(SensorData.ENERGY_CONSUMED);
    return String.format("%.05f", energy / 1000.0);
  }

  /**
   * Command-line program that will generate a CSV file containing energy information for one or
   * more sources over a given time period and at a given sampling interval.
   * 
   * @param args Server URI, username, and password to connect to the WattDepot server.
   */
  public static void main(String[] args) {
    if (args.length != 3 && args.length != 4) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    boolean getAllSources = true;
    if (args.length == 4) {
      getAllSources = false;
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
    EnergyMatrixExporter output = new EnergyMatrixExporter(client);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    if (getAllSources && !output.getAllSources()) {
      System.exit(1);
    }
    else if (!getAllSources && (!output.getNumSources(br) || !output.getSourceNames(br))) {
      System.exit(1);
    }

    if (!output.getDates(br) || !output.getSamplingInterval(br) || !output.printData()) {
      System.exit(1);
    }

  }

}
