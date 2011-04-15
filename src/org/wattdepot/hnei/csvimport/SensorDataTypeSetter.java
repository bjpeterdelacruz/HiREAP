package org.wattdepot.hnei.csvimport;

import static org.junit.Assert.fail;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class contains a main method that will store the type of data (daily or hourly) for a source
 * in the list of properties for that source.
 * 
 * @author BJ Peter DeLaCruz
 */
public class SensorDataTypeSetter {

  /** Used to connect to the WattDepot server. */
  private WattDepotClient client;

  /** Formats dates that are in the format MM/dd/yyyy. */
  private SimpleDateFormat formatDate;

  /** Start date for time interval. */
  private XMLGregorianCalendar startTimestamp;

  /** End date for time interval. */
  private XMLGregorianCalendar endTimestamp;

  /**
   * Creates a new SensorDataTypeSetter object.
   */
  public SensorDataTypeSetter() {
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
  }

  /**
   * Reads in URI, username, and password from a properties file, connects to a WattDepot server,
   * and then stores a test source.
   * 
   * @return true if connection is established, false otherwise.
   */
  public boolean setup() {
    DataInputClientProperties props = null;
    try {
      props = new DataInputClientProperties();
    }
    catch (IOException e) {
      System.out.println(e);
      fail();
    }

    String uri = props.get(WATTDEPOT_URI_KEY);
    String username = props.get(WATTDEPOT_USERNAME_KEY);
    String password = props.get(WATTDEPOT_PASSWORD_KEY);

    this.client = new WattDepotClient(uri, username, password);
    if (!this.client.isAuthenticated() || !this.client.isHealthy()) {
      System.out.println("Is authenticated? " + this.client.isAuthenticated());
      System.out.println("Is healthy? " + this.client.isHealthy());
      return false;
    }
    return true;
  }

  /**
   * Sets the type of data (daily or hourly) for a source and then updates the source stored on the
   * WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean processSources() {
    try {
      List<Source> sources = this.client.getSources();
      for (Source s : sources) {
        System.out.println("Setting type of data stored on " + s.getName() + "...");
        List<SensorData> datas =
            this.client.getSensorDatas(s.getName(), this.startTimestamp, this.endTimestamp);

        int numDays = 0;
        if (!datas.isEmpty()) {
          XMLGregorianCalendar start = datas.get(0).getTimestamp();
          XMLGregorianCalendar end = datas.get(datas.size() - 1).getTimestamp();
          numDays = Tstamp.daysBetween(start, end) + 1;
        }

        s.getProperties().getProperty().clear();
        s.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));

        if (datas.size() > numDays) {
          s.addProperty(new Property("dataType", "hourly"));
          System.out.println(s.getName() + ": [hourly]");
        }
        else if (datas.size() <= numDays && !datas.isEmpty()) {
          s.addProperty(new Property("dataType", "daily"));
          System.out.println(s.getName() + ": [daily]");
        }
        else {
          s.addProperty(new Property("dataType", "n/a"));
          System.out.println("No sensor data exists for " + s.getName());
        }

        this.client.storeSource(s, true);
      }
    }
    catch (JAXBException e) {
      e.printStackTrace();
      return false;
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Gets a date from the user via the command-line.
   * 
   * @return True if input is successful, false otherwise.
   */
  public boolean getDate() {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String command = null;

      System.out.print("Please enter a start date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in start timestamp.");
        return false;
      }
      this.startTimestamp = Tstamp.makeTimestamp(this.formatDate.parse(command).getTime());

      System.out.print("Please enter an end date in the format mm/dd/yyyy (e.g. 2/1/2011): ");
      if ((command = br.readLine()) == null) {
        System.out.println("Error encountered while trying to read in end timestamp.");
        return false;
      }
      this.endTimestamp = Tstamp.makeTimestamp(this.formatDate.parse(command).getTime());
    }
    catch (ParseException e) {
      e.printStackTrace();
      return false;
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * This command-line program will update the type of data that is stored for a source in the list
   * of properties for a source.
   * 
   * @param args None.
   */
  public static void main(String[] args) {
    SensorDataTypeSetter setter = new SensorDataTypeSetter();
    if (!setter.setup() || !setter.getDate() || !setter.processSources()) {
      System.exit(1);
    }
    System.out.println("Finished updating source information!");
  }

}
