package org.wattdepot.hnei.csvimport;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.text.ParseException;
import java.util.Date;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * JUnit tests for the HneiImporter class. The tests check:
 * 
 * >> If sources and sample data can be stored on the WattDepot server.
 * >> If the type of data (hourly or daily) is set correctly.
 * >> If sample data is monotonically increasing.
 * >> If the energy consumed computation is computed correctly.
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

  /** Error message. */
  private static final String ERROR_MESSAGE = "Unable to retrieve data.";

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
    if (secondTimestamp != null && secondReading != null) {
      this.storeTestData(secondTimestamp, secondReading, username);
    }
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
      this.client.deleteSensorData(this.sourceName, firstTstamp);
      if (secondTimestamp != null) {
        Date secondDate =
            ((HneiRowParser) this.importer.parser).formatDateTime.parse(secondTimestamp);
        XMLGregorianCalendar secondTstamp = Tstamp.makeTimestamp(secondDate.getTime());
        this.client.deleteSensorData(this.sourceName, secondTstamp);
      }
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

    this.setup(firstTimestamp, secondTimestamp, "3000", "4000");
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
      // assertEquals("number of sources is 2226", 2226, this.client.getSources().size());
      assertEquals("number of data is 2", 2, this.client.getSensorDatas(this.sourceName,
          firstTstamp, secondTstamp).size());
      this.teardown(firstTimestamp, secondTimestamp);
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
   * 
   * If the sampling interval is "daily," only one SensorData object should be in the list that is
   * returned by the call to the getSensorDatas method, which takes two parameters: the start time
   * (midnight) and end time (11:59:59 PM), both of which are on the same day. If the sampling
   * interval is "hourly," then two or more should be in the list.
   */
  @Test
  public void testSetTypeOfDataProperty() {
    String timestamp1 = "8/1/1999 9:00:00 AM";
    String timestamp2 = "8/1/1999 7:77:77 PM";
    String startTimestamp = "8/1/1999 12:00:00 AM";
    String endTimestamp = "8/1/1999 11:59:59 PM";

    Date date1 = null;
    Date date2 = null;
    Date startDate = null;
    Date endDate = null;
    Entry entry1 = null;
    Entry entry2 = null;
    SensorData data1 = null;
    SensorData data2 = null;
    XMLGregorianCalendar tstamp1 = null;
    XMLGregorianCalendar tstamp2 = null;
    XMLGregorianCalendar startTstamp = null;
    XMLGregorianCalendar endTstamp = null;

    this.setup(timestamp1, null, "30000", null);

    try {
      date1 = ((HneiRowParser) this.importer.parser).formatDateTime.parse(timestamp1);
      tstamp1 = Tstamp.makeTimestamp(date1.getTime());
      date2 = ((HneiRowParser) this.importer.parser).formatDateTime.parse(timestamp2);
      tstamp2 = Tstamp.makeTimestamp(date2.getTime());
      startDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(startTimestamp);
      startTstamp = Tstamp.makeTimestamp(startDate.getTime());
      endDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(endTimestamp);
      endTstamp = Tstamp.makeTimestamp(endDate.getTime());
      entry1 = new Entry(this.sourceName, null, tstamp1, null);
      entry2 = new Entry(this.sourceName, null, tstamp2, null);
    }
    catch (ParseException e) {
      System.out.println(e);
      fail();
    }

    /***** Test daily sensor data. *****/
    data1 = this.getSensorData(startTstamp, endTstamp, 0);
    if (data1 == null) {
      System.out.println(ERROR_MESSAGE);
      fail();
    }
    else {
      data1 = this.importer.setTypeOfDataProperty(this.client, entry1, data1);
      assertNotNull("data is daily", data1.getProperty("daily"));
      assertNull("data is not hourly", data1.getProperty("hourly"));
    }

    this.teardown(timestamp1, null);

    /***** Test hourly sensor data. *****/
    this.setup(timestamp1, timestamp2, "40000", "50000");

    data1 = this.getSensorData(startTstamp, endTstamp, 0);
    data2 = this.getSensorData(startTstamp, endTstamp, 1);
    if (data1 == null || data2 == null) {
      System.out.println(ERROR_MESSAGE);
      fail();
    }
    else {
      data1 = this.importer.setTypeOfDataProperty(this.client, entry1, data1);
      assertNotNull("data is hourly", data1.getProperty("hourly"));
      assertNull("data is not daily", data1.getProperty("daily"));
      data2 = this.importer.setTypeOfDataProperty(this.client, entry2, data2);
      assertNotNull("data is hourly", data2.getProperty("hourly"));
      assertNull("data is not daily", data2.getProperty("daily"));
    }

    this.teardown(timestamp1, timestamp2);
  }

  /**
   * Helper method that returns the SensorData object at a certain position in the list of
   * SensorData objects.
   * 
   * @param start Start timestamp for interval.
   * @param end End timestamp for interval.
   * @param idx Position in list at which SensorData object that is to be returned is located.
   * @return A SensorData object.
   */
  private SensorData getSensorData(XMLGregorianCalendar start, XMLGregorianCalendar end, int idx) {
    try {
      return this.client.getSensorDatas(this.sourceName, start, end).get(idx);
    }
    catch (WattDepotClientException e) {
      System.out.println(e);
      fail();
      return null;
    }
  }

  /**
   * Passes if property for monotonically increasing for each SensorData object is set properly.
   * 
   * Two SensorData objects are used. If the energy data for the second SensorData object, which has
   * a timestamp that is later than that of the first SensorData object, is less than the energy
   * data for the first SensorData object, the isMonotonicallyIncreasing property for the former
   * should be set to false. Otherwise, the isMonotonicallyIncreasing property for both objects
   * should be set to true.
   */
  @Test
  public void testSetMonotonicallyIncreasingProperty() {
    String timestamp1 = "8/1/1999 9:00:00 AM";
    String timestamp2 = "8/1/1999 7:77:77 PM";
    String startTimestamp = "8/1/1999 12:00:00 AM";
    String endTimestamp = "8/1/1999 11:59:59 PM";
    String isMonotonicallyIncreasing = "isMonotonicallyIncreasing";

    Date date1 = null;
    Date date2 = null;
    Date startDate = null;
    Date endDate = null;
    Entry entry1 = null;
    Entry entry2 = null;
    SensorData data1 = null;
    SensorData data2 = null;
    XMLGregorianCalendar tstamp1 = null;
    XMLGregorianCalendar tstamp2 = null;
    XMLGregorianCalendar startTstamp = null;
    XMLGregorianCalendar endTstamp = null;

    this.setup(timestamp1, timestamp2, "300000", "29999");

    try {
      date1 = ((HneiRowParser) this.importer.parser).formatDateTime.parse(timestamp1);
      tstamp1 = Tstamp.makeTimestamp(date1.getTime());
      date2 = ((HneiRowParser) this.importer.parser).formatDateTime.parse(timestamp2);
      tstamp2 = Tstamp.makeTimestamp(date2.getTime());
      startDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(startTimestamp);
      startTstamp = Tstamp.makeTimestamp(startDate.getTime());
      endDate = ((HneiRowParser) this.importer.parser).formatDateTime.parse(endTimestamp);
      endTstamp = Tstamp.makeTimestamp(endDate.getTime());
      entry1 = new Entry(this.sourceName, null, tstamp1, null);
      entry2 = new Entry(this.sourceName, null, tstamp2, null);
      data1 = this.getSensorData(startTstamp, endTstamp, 0);
      data2 = this.getSensorData(startTstamp, endTstamp, 1);
      if (data1 == null || data2 == null) {
        throw new WattDepotClientException(ERROR_MESSAGE);
      }
    }
    catch (ParseException e) {
      System.out.println(e);
      fail();
    }
    catch (WattDepotClientException e) {
      System.out.println(e);
      fail();
    }

    /***** Test with invalid energy data (non-monotonically increasing energy readings). *****/
    data1 = this.importer.setMonotonicallyIncreasingProperty(this.client, entry1, data1);
    data2 = this.importer.setMonotonicallyIncreasingProperty(this.client, entry2, data2);
    assertEquals("energy is increassing", "true", data1.getProperty(isMonotonicallyIncreasing));
    assertEquals("energy is decreasing", "false", data2.getProperty(isMonotonicallyIncreasing));

    this.teardown(timestamp1, timestamp2);

    /***** Test with valid energy data. *****/
    this.setup(timestamp1, timestamp2, "500", "5000");

    data1 = this.getSensorData(startTstamp, endTstamp, 0);
    data2 = this.getSensorData(startTstamp, endTstamp, 1);
    if (data1 == null || data2 == null) {
      System.out.println(ERROR_MESSAGE);
      fail();
    }
    else {
      data1 = this.importer.setMonotonicallyIncreasingProperty(this.client, entry1, data1);
      data2 = this.importer.setMonotonicallyIncreasingProperty(this.client, entry2, data2);
      assertEquals("energy is increasing", "true", data1.getProperty(isMonotonicallyIncreasing));
      assertEquals("energy is increasing", "true", data2.getProperty(isMonotonicallyIncreasing));
    }

    this.teardown(timestamp1, timestamp2);
  }

  /**
   * Passes if energy consumption over a given time period is computed correctly.
   */
  public void testEnergyConsumptionComputation() {
    // TODO: Finish method.
  }
}
