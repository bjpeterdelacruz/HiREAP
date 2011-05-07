package org.wattdepot.hnei.export;

import static org.junit.Assert.assertEquals;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_PASSWORD_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_URI_KEY;
import static org.wattdepot.datainput.DataInputClientProperties.WATTDEPOT_USERNAME_KEY;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.DataInputClientProperties;
import org.wattdepot.hnei.csvimport.SensorDataTypeSetter;
import org.wattdepot.hnei.csvimport.hnei.HneiImporter;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * JUnit tests for the QualityClassifier class. The tests check if sources are correctly classified
 * as Grade A, Grade B, or Grade C.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TestQualityClassifier {

  /** Used to store and retrieve sources and data on WattDepot server. */
  private static WattDepotClient client;

  /** Used to parse row of HNEI energy data. */
  private static HneiImporter importer;

  /** Start date. */
  private static final String START_DATE = "1999-01-01T06:00:00.000-10:00";

  /** End date. */
  private static final String END_DATE = "1999-12-31T23:59:59.000-10:00";

  /** List of source names. */
  private static final String[] SOURCE_NAMES = { "111111-1", "222222-2", "333333-3", "444444-4",
      "555555-5" };

  /** Some test data. */
  private static final String[][] SAMPLE_DATA = {
      { "994103718077", "8/1/2008", "111111", "1", "491", "35641", "035641",
          "7/1/1999 11:36:35 PM", "0" },
      { "994103718078", "8/2/2008", "111111", "1", "492", "35642", "035642",
          "7/2/1999 10:35:13 PM", "0" },
      { "994103718079", "8/3/2008", "111111", "1", "493", "35643", "035643", "7/3/1999 9:33:51 PM",
          "0" },
      { "994103718080", "8/4/2008", "222222", "2", "494", "35644", "035644",
          "7/1/1999 11:36:36 PM", "0" },
      { "994103718081", "8/5/2008", "222222", "2", "495", "35645", "035645",
          "7/2/1999 10:35:14 PM", "0" },
      { "994103718082", "8/6/2008", "222222", "2", "496", "35646", "035646", "7/3/1999 9:33:52 PM",
          "0" },
      { "994103718083", "8/7/2008", "333333", "3", "497", "35647", "035647",
          "7/1/1999 11:36:37 PM", "0" },
      { "994103718084", "8/8/2008", "333333", "3", "498", "35648", "035648",
          "7/2/1999 10:35:15 PM", "0" },
      { "994103718085", "8/9/2008", "333333", "3", "499", "35649", "035649", "7/3/1999 9:33:53 PM",
          "0" },
      { "994103718086", "8/10/2008", "444444", "4", "500", "35650", "035650",
          "7/1/1999 11:36:38 PM", "0" },
      { "994103718087", "8/11/2008", "444444", "4", "501", "35651", "035651",
          "7/2/1999 10:35:16 PM", "0" },
      { "994103718088", "8/12/2008", "444444", "4", "502", "35652", "035652", "7/3/1999 9:33:54 PM",
          "0" },
      { "994103718089", "8/13/2008", "555555", "5", "503", "35653", "035653",
          "7/1/1999 11:36:39 PM", "0" },
      { "994103718090", "8/14/2008", "555555", "5", "504", "35654", "035654",
          "7/2/1999 10:35:17 PM", "0" },
      { "994103718091", "8/15/2008", "555555", "5", "505", "35655", "035655", "7/3/1999 9:33:55 PM",
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

    TestQualityClassifier.client = new WattDepotClient(uri, username, password);
    if (!TestQualityClassifier.client.isAuthenticated()
        || !TestQualityClassifier.client.isHealthy()) {
      System.out.println("Is authenticated? " + TestQualityClassifier.client.isAuthenticated());
      System.out.println("Is healthy? " + TestQualityClassifier.client.isHealthy());
      throw new Exception();
    }
    TestQualityClassifier.importer = new HneiImporter(null, uri, username, password, false);
    // Store test source on WattDepot server.
    for (String source : SOURCE_NAMES) {
      TestQualityClassifier.importer.storeSource(TestQualityClassifier.client, source);
    }
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

    for (String source : SOURCE_NAMES) {
      List<SensorData> datas =
          TestQualityClassifier.client.getSensorDatas(source, startTstamp, endTstamp);
      if (!datas.isEmpty()) {
        for (SensorData d : datas) {
          TestQualityClassifier.client.deleteSensorData(source, d.getTimestamp());
        }
      }
    }
  }

  /**
   * Passes if all sources are classified as Grade A.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testGradeASources() throws Exception {
    QualityClassifier classifier = new QualityClassifier();
    classifier.setup();
    SensorData data = null;
    for (String[] row : SAMPLE_DATA) {
      data = TestQualityClassifier.importer.getParser().parseRow(row);
      TestQualityClassifier.importer.process(TestQualityClassifier.client, data);
    }
    classifier.dateBeforeStartDate = Tstamp.makeTimestamp("1999-07-01T00:00:00.000-10:00");
    classifier.startTimestamp = Tstamp.makeTimestamp("1999-07-02T00:00:00.000-10:00");
    classifier.endTimestamp = Tstamp.makeTimestamp("1999-07-02T23:59:59.000-10:00");
    classifier.dateAfterEndDate = Tstamp.makeTimestamp("1999-07-03T23:59:59.000-10:00");
    for (String source : SOURCE_NAMES) {
      classifier.sources.add(classifier.client.getSource(source));
    }
    this.setDataType(classifier);
    classifier.verifyData();
    assertEquals("All are Grade A sources", classifier.sources.size(),
        classifier.gradeA_DailySources.size());
  }

  /**
   * Passes if all sources are classified as Grade C.
   * 
   * @throws Exception if there are any problems.
   */
  @Test
  public void testGradeCSources() throws Exception {
    QualityClassifier classifier = new QualityClassifier();
    classifier.setup();
    SensorData data = null;
    for (String[] row : SAMPLE_DATA) {
      data = TestQualityClassifier.importer.getParser().parseRow(row);
      TestQualityClassifier.importer.process(TestQualityClassifier.client, data);
    }
    for (String source : SOURCE_NAMES) {
      classifier.sources.add(classifier.client.getSource(source));
    }

    System.out.println("Missing some data after start date.");
    classifier.dateBeforeStartDate = Tstamp.makeTimestamp("1999-06-01T00:00:00.000-10:00");
    classifier.startTimestamp = Tstamp.makeTimestamp("1999-06-02T00:00:00.000-10:00");
    classifier.endTimestamp = Tstamp.makeTimestamp("1999-07-02T23:59:59.000-10:00");
    classifier.dateAfterEndDate = Tstamp.makeTimestamp("1999-07-03T23:59:59.000-10:00");
    this.setDataType(classifier);
    classifier.verifyData();
    assertEquals("All are Grade C sources", classifier.sources.size(),
        classifier.gradeC_Sources.size());
    classifier.gradeC_Sources.clear();

    System.out.println("Missing some data before end date.");
    classifier.dateBeforeStartDate = Tstamp.makeTimestamp("1999-07-01T00:00:00.000-10:00");
    classifier.startTimestamp = Tstamp.makeTimestamp("1999-07-02T00:00:00.000-10:00");
    classifier.endTimestamp = Tstamp.makeTimestamp("1999-07-31T23:59:59.000-10:00");
    classifier.dateAfterEndDate = Tstamp.makeTimestamp("1999-08-01T23:59:59.000-10:00");
    this.setDataType(classifier);
    classifier.verifyData();
    assertEquals("All are Grade C sources", classifier.sources.size(),
        classifier.gradeC_Sources.size());
    classifier.gradeC_Sources.clear();
    this.deleteData();

    for (int idx = 0, reading = 100000; idx < SAMPLE_DATA.length; reading--, idx++) {
      SAMPLE_DATA[idx][6] = String.valueOf(reading);
      data = TestQualityClassifier.importer.getParser().parseRow(SAMPLE_DATA[idx]);
      TestQualityClassifier.importer.process(TestQualityClassifier.client, data);
    }

    System.out.println("Data is not monotonically increasing.");
    classifier.dateBeforeStartDate = Tstamp.makeTimestamp("1999-07-01T00:00:00.000-10:00");
    classifier.startTimestamp = Tstamp.makeTimestamp("1999-07-02T00:00:00.000-10:00");
    classifier.endTimestamp = Tstamp.makeTimestamp("1999-07-02T23:59:59.000-10:00");
    classifier.dateAfterEndDate = Tstamp.makeTimestamp("1999-07-03T23:59:59.000-10:00");
    this.setDataType(classifier);
    classifier.verifyData();
    assertEquals("All are Grade C sources", classifier.sources.size(),
        classifier.gradeC_Sources.size());
  }

  /**
   * Sets the type of data (hourly or daily) for the test data.
   * 
   * @param classifier Used to get the WattDepotClient object, list of sources, and start and end
   * dates.
   */
  public void setDataType(QualityClassifier classifier) {
    SensorDataTypeSetter setter = new SensorDataTypeSetter();
    setter.setClient(classifier.client);
    setter.setSources(classifier.sources);
    setter.setStartTimestamp(classifier.startTimestamp);
    setter.setEndTimestamp(classifier.endTimestamp);
    setter.processSources();
  }

}
