package org.wattdepot.hnei.csvimport;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import java.text.ParseException;
import java.util.logging.Logger;
import org.junit.Test;
import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * JUnit tests for the HneiRowParser class.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TestHneiRowParser {

  /** */
  public static final String NULL_MESSAGE = "data is null";

  /**
   * Returns a new HneiRowParser object.
   * 
   * @return A new HneiRowParser object.
   */
  public HneiRowParser setupParser() {
    HneiRowParser parser =
        new HneiRowParser("HneiRowParser", "http://localhost:8182/wattdepot/", "1951005-1", null);
    parser.log = Logger.getLogger(HneiImporter.class.getName());
    return parser;
  }

  /**
   * Returns a valid row for testing.
   * 
   * @return A valid row for testing.
   */
  public String[] setupRow() {
    String[] row =
        { "994515990077", "8/1/2009", "1951005", "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    return row;
  }

  /**
   * Should pass if a row with an invalid length is passed into the parseRow method.
   */
  @Test
  public void testParseRowInvalidRowLength() {
    String[] row = new String[5];
    SensorData data = setupParser().parseRow(row);
    assertNull(NULL_MESSAGE, data);
  }

  /**
   * Should pass if a row with no energy data is passed into the parseRow method.
   */
  @Test
  public void testParseRowNoReadings() {
    String[] row = setupRow();
    HneiRowParser parser = setupParser();

    row[5] = "NO READING";
    SensorData data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[5] = "35958";
    data = parser.parseRow(row);
    assertNotNull("data is not null", data);
    row[6] = "no reading";
    data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[6] = "035958";
    data = parser.parseRow(row);
    assertNotNull("data is not null", data);
    assertEquals("number of readings is 2", 2, parser.getNumNoReadings());
  }

  /**
   * Should pass if a row with invalid fields is passed into the parseRow method.
   */
  @Test
  public void testParseRowInvalidFields() {
    String[] row = setupRow();
    HneiRowParser parser = setupParser();

    row[2] = "";
    SensorData data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[2] = null;
    data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    row[2] = "Z33";
    data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    assertEquals("number of blank values is 2", 2, parser.getNumBlankValues());
    assertEquals("number of non-numeric values is 1", 1, parser.getNumNonnumericValues());
  }

  /**
   * Should pass if the dates in the row are valid.
   */
  @Test
  public void testValidDates() {
    String[] row = setupRow();
    HneiRowParser parser = setupParser();

    try {
      parser.formatDate.parse(row[1]);
    }
    catch (ParseException e) {
      fail();
    }

    row[1] = "8/1/2009 12:00:00 AM";
    try {
      parser.formatDateTime.parse(row[1]);
    }
    catch (ParseException e) {
      fail();
    }

    System.out.println("testValidDates: PASSED");
  }

  /**
   * Should pass if SensorData object has properties attached to it.
   */
  @Test
  public void testDataProperties() {
    String[] row = setupRow();
    HneiRowParser parser = setupParser();
    SensorData data = parser.parseRow(row);

    // Check number of properties.
    assertEquals("size is 11", 11, data.getProperties().getProperty().size());
    // Check source name.
    String mtu = data.getProperty("mtuID");
    String port = data.getProperty("port");
    String sourceName = parser.getServerUri() + "sources/" + mtu + "-" + port;
    assertEquals("sourceName is " + sourceName, sourceName, data.getSource());
    // Check if energy data is valid.
    row[6] = "-777";
    data = parser.parseRow(row);
    assertNull(NULL_MESSAGE, data);
    // Check if energy data is stored in SensorData object.
    row[6] = "1000";
    data = parser.parseRow(row);
    double energy = data.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
    assertEquals("energy is " + energy, new Double(1000000.0), new Double(energy));
  }
}
