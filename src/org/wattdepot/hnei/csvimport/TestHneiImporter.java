package org.wattdepot.hnei.csvimport;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import java.text.ParseException;
import java.util.Date;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * JUnit tests for the HneiImporter class.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TestHneiImporter {

  /** Used to store and retrieve sources and data on WattDepot server. */
  private WattDepotClient client;

  /** Used to parse row of HNEI energy data. */
  private HneiImporter importer;

  /** Name of test source. */
  private String sourceName = "777777-7";

  /**
   * Connects to the WattDepot server, stores a test source on it, and finally stores some test data
   * on the server.
   * 
   * @param firstTimestamp Timestamp for first test data.
   * @param secondTimestamp Timestamp for second test data.
   * @param firstReading Reading for first test data.
   * @param secondReading Reading for second test data.
   */
  public void setup(String firstTimestamp, String secondTimestamp, String firstReading,
      String secondReading) {
    String uri = "http://localhost:8182/wattdepot/";
    String username = "admin@example.com";
    String password = "admin@example.com";

    this.client = new WattDepotClient(uri, username, password);
    if (!this.client.isAuthenticated() || !this.client.isHealthy()) {
      System.out.println("Is authenticated? " + this.client.isAuthenticated());
      System.out.println("Is healthy? " + this.client.isHealthy());
      fail();
    }

    this.importer = new HneiImporter(null, uri, username, password, false);
    this.storeTestSource();
    this.storeTestData(firstTimestamp, firstReading, username);
    this.storeTestData(secondTimestamp, secondReading, username);
  }

  /**
   * Stores a source on the WattDepot server.
   */
  private void storeTestSource() {
    this.importer.setSourceName(this.sourceName);
    this.importer.storeSource(this.client);
  }

  /**
   * Stores a SensorData object on the WattDepot server.
   * 
   * @param timestamp Timestamp for test data.
   * @param reading Reading for test data.
   * @param username Username for authentication.
   */
  private void storeTestData(String timestamp, String reading, String username) {
    SensorData data = getSensorData((HneiRowParser) this.importer.parser, timestamp, reading);
    this.importer.process(this.client, new Source(this.sourceName, username, true), data);
  }

  /**
   * Returns a SensorData object given a row and custom timestamp and reading used for testing.
   * 
   * @param parser HneiRowParser, used to return a SensorData object given a row.
   * @param timestamp Timestamp for test data.
   * @param reading Reading for test data.
   * 
   * @return A SensorData object given a row and custom timestamp and reading used for testing.
   */
  private SensorData getSensorData(HneiRowParser parser, String timestamp, String reading) {
    String[] row =
        { "994515990077", "8/1/1999 8:00:00 AM", "777777", "7", "491", reading, reading, timestamp,
            "0" };
    return parser.parseRow(row);
  }

  /**
   * Deletes test data from WattDepot server.
   * 
   * @param firstTimestamp Timestamp for first SensorData object.
   * @param secondTimestamp Timestamp for second SensorData object.
   */
  public void teardown(String firstTimestamp, String secondTimestamp) {
    try {
      Date firstDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(firstTimestamp);
      XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(firstDate.getTime());
      Date secondDate =
          ((HneiRowParser) this.importer.parser).formatDateTime.parse(secondTimestamp);
      XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(secondDate.getTime());
      this.client.deleteSensorData(this.sourceName, firstTstamp);
      this.client.deleteSensorData(this.sourceName, secondTstamp);
    }
    catch (ParseException e) {
      System.out.println(e);
      fail();
    }
    catch (WattDepotClientException e) {
      System.out.println(e);
      fail();
    }
  }

  /**
   * Passes if the counts for the number of sources and sensor data stored on the server are
   * correct.
   */
  @Test
  public void sanityCheck() {
    String firstTimestamp = "8/1/1999 9:00:00 AM";
    String secondTimestamp = "8/2/1999 1:00:00 AM";

    Date firstDate = null;
    Date secondDate = null;
    XMLGregorianCalendar firstTstamp = null;
    XMLGregorianCalendar secondTstamp = null;

    setup(firstTimestamp, secondTimestamp, "30000", "40000");

    try {
      firstDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(firstTimestamp);
      firstTstamp = Tstamp.makeTimestamp(firstDate.getTime());
      secondDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(secondTimestamp);
      secondTstamp = Tstamp.makeTimestamp(secondDate.getTime());
    }
    catch (ParseException e) {
      System.out.println(e);
      fail();
    }

    try {
      assertEquals("number of sources is 2226", 2226, this.client.getSources().size());
      assertEquals("number of data is 2", 2, this.client.getSensorDatas(this.sourceName,
          firstTstamp, secondTstamp).size());
      teardown(firstTimestamp, secondTimestamp);
      assertEquals("number of data is 0", 0, this.client.getSensorDatas(this.sourceName,
          firstTstamp, secondTstamp).size());
    }
    catch (WattDepotClientException e) {
      System.out.println(e);
      fail();
    }
  }

  /**
   * Passes if the sampling interval is set properly.
   */
  public void testSetSamplingIntervalProperty() {
    // TODO: Finish method.
  }

  /**
   * Passes if property for monotonically increasing is set properly.
   */
  public void testSetMonotonicallyIncreasingProperty() {
    // TODO: Finish method.
  }

  /**
   * Passes if energy consumption over a given time period is computed correctly.
   */
  public void testEnergyConsumptionComputation() {
    // TODO: Finish method.
  }
}
