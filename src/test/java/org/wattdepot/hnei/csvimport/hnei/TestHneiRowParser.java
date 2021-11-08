package org.wattdepot.hnei.csvimport.hnei;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * JUnit tests for the HneiRowParser class.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TestHneiRowParser {

  private static HneiRowParser parser;

  /** Displays the message <code>data is null</code>. */
  private static final String NULL_MESSAGE = "data is null";

  /** Name of test source. */
  private static final String SOURCE_NAME = "1726570-1";

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
    TestHneiRowParser.parser = new HneiRowParser("TestHneiRowParser", uri, SOURCE_NAME, null);
    TestHneiRowParser.parser.log = Logger.getLogger(HneiRowParser.class.getName());
  }

  /**
   * Returns a valid row for testing.
   * 
   * @return A valid row for testing.
   */
  private String[] setupRow() {
    String[] row =
        { "994515990077", "8/1/2009", "1951005", "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    return row;
  }

  /**
   * Should pass if a row with an invalid length is passed into the parseRow method.
   */
  @Test
  public void testInvalidRowLength() {
    String[] row = new String[5];
    SensorData data = TestHneiRowParser.parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
  }

  /**
   * Should pass if a row with no energy data is passed into the parseRow method.
   */
  @Test
  public void testNoReadings() {
    String[] row = setupRow();

    row[5] = "NO READING";
    SensorData data = TestHneiRowParser.parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[5] = "35958";
    data = TestHneiRowParser.parser.parseRow(row);
    assertNotNull("data is not null", data);
    row[6] = "no reading";
    data = TestHneiRowParser.parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[6] = "035958";
    data = TestHneiRowParser.parser.parseRow(row);
    assertNotNull("data is not null", data);
    assertEquals("number of readings is 2", 2, TestHneiRowParser.parser.getNumNoReadings());
  }

  /**
   * Should pass if a row with invalid fields is passed into the parseRow method.
   */
  @Test
  public void testInvalidFields() {
    String[] row1 =
        { "994515990077", "8/1/2009", "", "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    SensorData data = TestHneiRowParser.parser.parseRow(row1);
    assertNull(NULL_MESSAGE, data);

    String[] row2 =
        { "994515990077", "8/1/2009", null, "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    data = TestHneiRowParser.parser.parseRow(row2);
    assertNull(NULL_MESSAGE, data);

    String[] row3 =
        { "994515990077", "8/1/2009", "Z33", "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    data = TestHneiRowParser.parser.parseRow(row3);
    assertNull(NULL_MESSAGE, data);
    assertEquals("number of blank values is 2", 2, TestHneiRowParser.parser.getNumBlankValues());
    int result = TestHneiRowParser.parser.getNumNonnumericValues();
    assertEquals("number of non-numeric values is 1", 1, result);
  }

  /**
   * Should pass if the dates in the row are valid.
   */
  @Test
  public void testValidDates() {
    String[] row = setupRow();

    try {
      TestHneiRowParser.parser.formatDate.parse(row[1]);
    }
    catch (ParseException e) {
      fail();
    }

    row[1] = "8/1/2009 12:00:00 AM";
    try {
      TestHneiRowParser.parser.formatDateTime.parse(row[1]);
    }
    catch (ParseException e) {
      fail();
    }
  }

  /**
   * Should pass if SensorData object has properties attached to it.
   */
  @Test
  public void testDataProperties() {
    String[] row = setupRow();
    SensorData data = TestHneiRowParser.parser.parseRow(row);

    // Check number of properties.
    assertEquals("size is 1", 1, data.getProperties().getProperty().size());
    // Check source name.
    String mtu = row[2];
    String port = row[3];
    String sourceName = TestHneiRowParser.parser.getServerUri() + "sources/" + mtu + "-" + port;
    assertEquals("sourceName is " + sourceName, sourceName, data.getSource());
    // Check if energy data is valid.
    row[6] = "-777";
    data = TestHneiRowParser.parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    // Check if energy data is stored in SensorData object.
    row[6] = "1000";
    data = TestHneiRowParser.parser.parseRow(row);
    double energy = data.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    assertEquals("energy is " + energy, new Double(1000000.0), new Double(energy));
  }

}
