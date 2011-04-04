package org.wattdepot.hnei.csvimport;

import static org.junit.Assert.fail;
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

  /** */
  private WattDepotClient client;

  /** */
  private HneiImporter importer;

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
    String sourceName = "JUnitTestSource";

    this.client = new WattDepotClient(uri, username, password);
    if (!this.client.isAuthenticated() || !this.client.isHealthy()) {
      fail();
    }

    this.importer = new HneiImporter(null, uri, username, password, false);
    this.storeTestSource(sourceName);
    this.storeTestData(firstTimestamp, firstReading, sourceName, username);
    this.storeTestData(secondTimestamp, secondReading, sourceName, username);
  }

  /**
   * Stores a source on the WattDepot server.
   * 
   * @param sourceName Test source.
   */
  private void storeTestSource(String sourceName) {
    this.importer.setSourceName(sourceName);
    this.importer.storeSource(this.client);
  }

  /**
   * Stores a SensorData object on the WattDepot server.
   * 
   * @param timestamp Timestamp for test data.
   * @param reading Reading for test data.
   * @param sourceName Test source.
   * @param username Username for authentication.
   */
  private void storeTestData(String timestamp, String reading, String sourceName, String username) {
    SensorData data = getSensorData((HneiRowParser) this.importer.parser, timestamp, reading);
    this.importer.process(this.client, new Source(sourceName, username, true), data);
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
        { "994515990077", "8/1/1999 8:00:00 AM", "1951005", "1", "491", reading, reading,
            timestamp, "0" };
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
      String sourceName = "JUnitTestSource";
      Date firstDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(firstTimestamp);
      XMLGregorianCalendar firstTstamp = Tstamp.makeTimestamp(firstDate.getTime());
      Date secondDate =
          ((HneiRowParser) this.importer.parser).formatDateTime.parse(secondTimestamp);
      XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(secondDate.getTime());
      this.client.deleteSensorData(sourceName, firstTstamp);
      this.client.deleteSensorData(sourceName, secondTstamp);
    }
    catch (ParseException e) {
      fail();
    }
    catch (WattDepotClientException e) {
      fail();
    }
  }

  /**
   * Passes if the counts for the number of sources and sensor data stored on the server are
   * correct.
   */
  public void sanityCheck() {
    String firstTimestamp = "8/1/1999 9:00:00 AM";
    String secondTimestamp = "8/2/1999 1:00:00 AM";
    setup(firstTimestamp, secondTimestamp, "30000", "40000");
    teardown(firstTimestamp, secondTimestamp);
  }

  /**
   * Tests the HneiImporter class.
   */
  @Test
  public void testImporter() {
    int x = 25;
    System.out.println(x);
  }
}
