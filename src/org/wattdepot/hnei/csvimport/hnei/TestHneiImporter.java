package org.wattdepot.hnei.csvimport.hnei;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
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

  /** MTU of test source. */
  private static final String MTU = "111111";

  /** Start date. */
  private static final String START_DATE = "1999-01-01T06:00:00.000-10:00";

  /** End date. */
  private static final String END_DATE = "1999-12-31T23:59:59.000-10:00";

  /** Some test data. */
  private static final String[][] SAMPLE_DATA =
      {
          { "994103718077", "8/1/2008", MTU, "1", "491", "35641", "035641", "7/1/1999 11:36:35 PM",
              "0" },
          { "994103718078", "8/2/2008", MTU, "1", "492", "35640", "035640", "7/1/1999 10:35:13 PM",
              "0" },
          { "994103718079", "8/3/2008", MTU, "1", "493", "35638", "035638", "7/1/1999 9:33:51 PM",
              "0" },
          { "994103718080", "8/4/2008", MTU, "1", "494", "35636", "035636", "7/1/1999 8:32:29 PM",
              "0" },
          { "994103718081", "8/5/2008", MTU, "1", "495", "35633", "035633", "7/1/1999 7:31:07 PM",
              "0" } };

  /**
   * Reads in URI, username, and password from a properties file, connects to a WattDepot server,
   * and then stores a test source.
   * 
   * @throws Exception if unable to connect to WattDepot server.
   */
  @BeforeClass
  public static void setup() throws Exception {
    DataInputClientProperties props = new DataInputClientProperties();

    String uri = props.get(WATTDEPOT_URI_KEY);
    String username = props.get(WATTDEPOT_USERNAME_KEY);
    String password = props.get(WATTDEPOT_PASSWORD_KEY);

    TestHneiImporter.client = new WattDepotClient(uri, username, password);
    if (!TestHneiImporter.client.isAuthenticated() || !TestHneiImporter.client.isHealthy()) {
      System.out.println("Is authenticated? " + TestHneiImporter.client.isAuthenticated());
      System.out.println("Is healthy? " + TestHneiImporter.client.isHealthy());
      throw new Exception();
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
    XMLGregorianCalendar startTstamp = Tstamp.makeTimestamp(START_DATE);
    XMLGregorianCalendar endTstamp = Tstamp.makeTimestamp(END_DATE);

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

  /**
   * Passes if sensor data is successfully stored on WattDepot server.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testNumberOfDataImported() throws Exception {
    SensorData[] datas = new SensorData[SAMPLE_DATA.length];

    for (int idx = 0; idx < SAMPLE_DATA.length; idx++) {
      datas[idx] = TestHneiImporter.importer.getParser().parseRow(SAMPLE_DATA[idx]);
      assertNotNull("data is not null", datas[idx]);
      TestHneiImporter.importer.process(TestHneiImporter.client, datas[idx]);
    }

    XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(START_DATE);
    XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(END_DATE);

    List<SensorData> results =
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, firstTstamp, secondTstamp);
    assertEquals("results is 5", 5, results.size());
  }

  /**
   * Passes if energy is monotonically increasing.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testValidData() throws Exception {
    SensorData[] datas = new SensorData[SAMPLE_DATA.length];

    for (int idx = 0; idx < SAMPLE_DATA.length; idx++) {
      datas[idx] = TestHneiImporter.importer.getParser().parseRow(SAMPLE_DATA[idx]);
      assertNotNull("data is not null", datas[idx]);
      TestHneiImporter.importer.process(TestHneiImporter.client, datas[idx]);
    }

    XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(START_DATE);
    XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(END_DATE);

    List<SensorData> results =
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, firstTstamp, secondTstamp);
    assertEquals("results is 5", 5, results.size());

    MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();
    validator.setDatas(results);
    Entry entry = null;
    for (SensorData d : datas) {
      entry = new Entry(SOURCE_NAME, null, d.getTimestamp(), null);
      validator.setCurrentData(d);
      assertTrue("data is monotonically increasing", validator.validateEntry(entry));
    }
  }

  /**
   * Passes if energy is not monotonically increasing.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testInvalidData() throws Exception {
    SensorData[] datas = new SensorData[SAMPLE_DATA.length];

    for (int idx = 0, reading = 50000; idx < SAMPLE_DATA.length; idx++, reading++) {
      SAMPLE_DATA[idx][6] = Integer.toString(reading);
      datas[idx] = TestHneiImporter.importer.getParser().parseRow(SAMPLE_DATA[idx]);
      assertNotNull("data is not null", datas[idx]);
      TestHneiImporter.importer.process(TestHneiImporter.client, datas[idx]);
    }

    XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(START_DATE);
    XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(END_DATE);

    List<SensorData> results =
        TestHneiImporter.client.getSensorDatas(SOURCE_NAME, firstTstamp, secondTstamp);
    assertEquals("results is 5", 5, results.size());

    MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue();
    validator.setDatas(results);
    Entry entry = null;
    for (int idx = 0; idx < SAMPLE_DATA.length; idx++) {
      entry = new Entry(SOURCE_NAME, null, results.get(idx).getTimestamp(), null);
      validator.setCurrentData(results.get(idx));
      if (idx > 0) {
        assertFalse("data is not monotonically increasing", validator.validateEntry(entry));
      }
      else {
        assertTrue("data is monotonically increasing", validator.validateEntry(entry));
      }
    }
  }

}
