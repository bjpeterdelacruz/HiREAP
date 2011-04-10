package org.wattdepot.hnei.csvimport.hnei;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;

/**
 * JUnit tests for the HneiImporter class. The tests check:
 * 
 * <ul>
 * <li>If sources and sample data can be stored on the WattDepot server.</li>
 * <li>If the energy consumed computation is computed correctly.</li>
 * </ul>
 * 
 * @author BJ Peter DeLaCruz
 */
public class TestHneiImporter {

  /** Used to store and retrieve sources and data on WattDepot server. */
  private static WattDepotClient client;

  /** Used to parse row of HNEI energy data. */
  private static HneiImporter importer;

  /** Name of test source. */
  private static final String SOURCE_NAME = "111111-1";

  /**
   * Reads in URI, username, and password from properties file, starts up the WattDepot server, and
   * then stores a test source.
   */
  @BeforeClass
  public static void setup() {
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

    TestHneiImporter.client = new WattDepotClient(uri, username, password);
    if (!TestHneiImporter.client.isAuthenticated() || !TestHneiImporter.client.isHealthy()) {
      System.out.println("Is authenticated? " + TestHneiImporter.client.isAuthenticated());
      System.out.println("Is healthy? " + TestHneiImporter.client.isHealthy());
      fail();
    }
    TestHneiImporter.importer = new HneiImporter(null, uri, username, password, false);
    // Store test source on WattDepot server.
    TestHneiImporter.importer.storeSource(TestHneiImporter.client, SOURCE_NAME);
  }

  /**
   * Deletes test data from WattDepot server.
   * 
   * @throws Exception if problems are encountered trying to delete sensor data from server.
   */
  @After
  public void deleteData() throws Exception {
    String startTimestamp = "1999-01-01T00:00:00.000-10:00";
    String endTimestamp = "1999-12-31T11:59:59.000-10:00";

    XMLGregorianCalendar startTstamp = Tstamp.makeTimestamp(startTimestamp);
    XMLGregorianCalendar endTstamp = Tstamp.makeTimestamp(endTimestamp);

    List<SensorData> datas =
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, startTstamp, endTstamp);
    if (!datas.isEmpty()) {
      for (SensorData d : datas) {
        TestHneiImporter.client.deleteSensorData(SOURCE_NAME, d.getTimestamp());
      }
    }
  }

  /**
   * Stores test data on the WattDepot server.
   * 
   * @throws Exception if there are any problems.
   * @param timestamp Timestamp for test data.
   * @param reading Reading for test data in kWh.
   */
  private void putData(String timestamp, String reading) throws Exception {
    String[] row =
        { "994515990077", "8/1/1999 8:00:00 AM", "111111", "1", "491", reading, reading, timestamp,
            "0" };

    SensorData data = TestHneiImporter.importer.getParser().parseRow(row);
    TestHneiImporter.client.storeSensorData(data);
  }

  /**
   * Passes if the counts for the number of sources and sensor data stored on the server are
   * correct.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void sanityCheck() throws Exception {
    String firstDataTimestamp = "1999-07-01T06:00:00.000-10:00";
    String secondDataTimestamp = "1999-07-11T06:00:00.000-10:00";

    XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(firstDataTimestamp);
    XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(secondDataTimestamp);

    this.putData("7/1/1999 6:00:00 AM", "3000");
    this.putData("7/11/1999 6:00:00 AM", "6000");

    assertEquals("number of data is 2", 2,
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, firstTstamp, secondTstamp).size());
    this.deleteData();
    assertEquals("number of data is 0", 0,
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, firstTstamp, secondTstamp).size());
  }

  /**
   * Passes if energy consumption over a given time period is computed correctly.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testEnergyConsumptionComputation() throws Exception {
    String firstDataTimestamp = "1999-07-01T06:00:00.000-10:00";
    String secondDataTimestamp = "1999-07-11T06:00:00.000-10:00";

    String startTimestamp = "1999-07-05T06:00:00.000-10:00";
    String endTimestamp = "1999-07-06T06:00:00.000-10:00";

    XMLGregorianCalendar tstamp1 = Tstamp.makeTimestamp(firstDataTimestamp);
    XMLGregorianCalendar tstamp2 = Tstamp.makeTimestamp(secondDataTimestamp);
    XMLGregorianCalendar startTstamp = Tstamp.makeTimestamp(startTimestamp);
    XMLGregorianCalendar endTstamp = Tstamp.makeTimestamp(endTimestamp);

    this.putData("7/1/1999 6:00:00 AM", "3000");
    this.putData("7/11/1999 6:00:00 AM", "6000");

    System.out.println("\n" + TestHneiImporter.client.getSensorData(SOURCE_NAME, tstamp1));
    System.out.println("\n" + TestHneiImporter.client.getSensorData(SOURCE_NAME, tstamp2));

    System.out.println();
    System.out.println(TestHneiImporter.client.getEnergyConsumed(SOURCE_NAME, startTstamp,
        endTstamp, 60));
  }
}
