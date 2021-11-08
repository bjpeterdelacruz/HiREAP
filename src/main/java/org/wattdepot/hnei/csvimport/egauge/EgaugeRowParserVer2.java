package org.wattdepot.hnei.csvimport.egauge;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.hnei.csvimport.hnei.HneiRowParser;
import org.wattdepot.hnei.csvimport.validation.NonblankValue;
import org.wattdepot.hnei.csvimport.validation.NumericValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used to parse CSV files containing Egauge data for one source provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EgaugeRowParserVer2 extends HneiRowParser {

  /**
   * Creates a new EgaugeRowParser object.
   * 
   * @param toolName Name of the program.
   * @param serverUri URI of WattDepot server.
   * @param sourceName Source that is described by the sensor data.
   * @param log Log file, created in the HneiTabularFileSensor class.
   */
  public EgaugeRowParserVer2(String toolName, String serverUri, String sourceName, Logger log) {
    super(toolName, serverUri, sourceName, log);
    this.formatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US);
  }

  /**
   * Parses a row of Egauge data for a source from a CSV file provided by HNEI.
   * 
   * @param col Row from a CSV file that contains Egauge data.
   * @return SensorData object if parse is successful, null otherwise.
   */
  @Override
  public SensorData parseRow(String[] col) {
    if (col == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    if (col.length != 19 && col.length != 21) {
      String msg = "Row not in specified format:\n" + rowToString(col);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    // Run validations.
    boolean result;
    for (int i = 1; i < col.length; i++) {
      for (Validator v : validators) {
        result = v.validateEntry(col[i]);
        if (!result) {
          String msg = "[" + col[i] + "] " + v.getErrorMessage() + "\n" + rowToString(col);
          System.err.print(msg);
          this.log.log(Level.WARNING, msg);
        }
        if (v instanceof NonblankValue && !result) {
          numBlankValues++;
          return null;
        }
        if (v instanceof NumericValue && !result) {
          numNonnumericValues++;
          return null;
        }
      }
    }

    // Convert kW --> W
    double[] values = new double[col.length - 1];
    for (int index = 0, n = 1; index < values.length; index++, n++) {
      values[index] = Double.parseDouble(col[n]) * 1000;
    }

    Date date = null;
    try {
      date = formatDateTime.parse(col[0]);
    }
    catch (ParseException e) {
      try {
        date = formatDate.parse(col[0]);
      }
      catch (ParseException pe) {
        String msg = "Bad timestamp found in input file: " + col[0] + "\n" + rowToString(col);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(date.getTime());

    SensorData data = null;

    // Store energy data first.
    Property powerConsumed = new Property(SensorData.ENERGY_CONSUMED_TO_DATE, values[0]);
    String sourceUri = Source.sourceToUri(this.sourceName, this.serverUri);
    data = new SensorData(timestamp, this.toolName, sourceUri, powerConsumed);

    data.addProperty(new Property(SensorData.ENERGY_GENERATED, values[1]));
    int index = 0;
    if (col.length == 21) {
      data.addProperty(new Property("grid1-energy", values[2]));
      data.addProperty(new Property("grid2-energy", values[3]));
      index = 3;
    }
    else {
      data.addProperty(new Property("grid-energy", values[2]));
      index = 2;
    }
    data.addProperty(new Property("airConditioner1-energy", values[index + 1]));
    data.addProperty(new Property("airConditioner2-energy", values[index + 2]));
    data.addProperty(new Property("dhw1-energy", values[index + 3]));
    data.addProperty(new Property("dhw2-energy", values[index + 4]));
    data.addProperty(new Property("dryer1-energy", values[index + 5]));
    data.addProperty(new Property("dryer2-energy", values[index + 6]));

    // Then store power data.
    data.addProperty(new Property(SensorData.POWER_CONSUMED, values[index + 7]));
    data.addProperty(new Property(SensorData.POWER_GENERATED, values[index + 8]));
    if (col.length == 21) {
      data.addProperty(new Property("grid1-power", values[index + 9]));
      data.addProperty(new Property("grid2-power", values[index + 10]));
      index = index + 10;
    }
    else {
      data.addProperty(new Property("grid-power", values[index + 9]));
      index = index + 9;
    }
    data.addProperty(new Property("airConditioner1-power", values[index + 1]));
    data.addProperty(new Property("airConditioner2-power", values[index + 2]));
    data.addProperty(new Property("dhw1-power", values[index + 3]));
    data.addProperty(new Property("dhw2-power", values[index + 4]));
    data.addProperty(new Property("dryer1-power", values[index + 5]));
    data.addProperty(new Property("dryer2-power", values[index + 6]));

    return data;
  }

  /**
   * Test program to see if row parser works.
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
    System.out.println("Successfully connected to " + client.getWattDepotUri() + ".\n");

    String sourceName = "6363-A_Paro";
    EgaugeRowParserVer2 parser =
        new EgaugeRowParserVer2("EgaugeRowParserVer2", serverUri, sourceName, null);

    String noData = "0.000000000";
    String[] col1 =
        { "2011-02-07 13:49", "1649.596900278", noData, "1288.171166667", "1212.671317222",
            "113.022624722", "533.594684444", "-36.533478889", "-12.680254167", "-0.058363056",
            "-81.699830556", "0.480333333", noData, "0.189333333", "0.366466667",
            "0.002000000", "0.001000000", "-0.029433333", "-0.034283333", noData,
            noData };
    String[] col2 =
    { "2011-02-07 15:49", "1649.604736944", noData, "1288.171166667", "1212.671317222",
        "113.022624722", "533.594684444", "-36.533478889", "-12.680254167", "-0.058363056",
        "-81.699830556", "0.470200000", "", "0.189333333", "0.366466667",
        "0.001000000", "0.002000000", "-0.029433333", "-0.034283333", noData,
        noData };

    SensorData data = null;
    try {
      client.storeSource(new Source(sourceName, username, true), true);
      data = parser.parseRow(col1);
      client.storeSensorData(data);
      data = parser.parseRow(col2);
      client.storeSensorData(data);
    }
    catch (WattDepotClientException e) {
      System.err.println(e);
    }
    catch (JAXBException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Date date1 = null;
    Date date2 = null;
    try {
      date1 = parser.formatDateTime.parse("2011-02-07 13:49");
      date2 = parser.formatDateTime.parse("2011-02-07 15:49");
    }
    catch (ParseException e) {
      e.printStackTrace();
      System.exit(1);
    }
    XMLGregorianCalendar timestamp1 = Tstamp.makeTimestamp(date1.getTime());
    XMLGregorianCalendar timestamp2 = Tstamp.makeTimestamp(date2.getTime());

    System.out.println("Data:");
    try {
      System.out.println(client.getSensorData(sourceName, timestamp1));
      System.out.println(client.getSensorData(sourceName, timestamp2));
      XMLGregorianCalendar start = Tstamp.incrementHours(timestamp1, 1);
      XMLGregorianCalendar end = Tstamp.incrementHours(timestamp2, 0);
      System.out.print("Energy consumed (Wh): ");
      System.out.println(client.getEnergyConsumed(sourceName, start, end, 15));
      client.deleteSensorData(sourceName, timestamp1);
      client.deleteSensorData(sourceName, timestamp2);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
