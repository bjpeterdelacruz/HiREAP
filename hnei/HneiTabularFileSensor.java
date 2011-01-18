package hnei;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.datainput.RowParseException;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads data from CSV files provided by HNEI (delimited by commas), creates a SensorData object
 * for each line, object for each line, and sends the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiTabularFileSensor {

	  /** Name of the file to be input. */
	  protected String filename;

	  /** URI of WattDepot server to send data to. */
	  protected String serverUri;

	  /** Name of Source to send data to. */
	  protected String sourceName;

	  /** Username to use when sending data to server. */
	  protected String username;

	  /** Password to use when sending data to server. */
	  protected String password;

	  /** Whether or not to skip the first row of the file. */
	  protected boolean skipFirstRow;

	  /** Name of the application on the command line. */
	  protected final String toolName = "HneiTabularFileSensor";

	  /** The parser used to turn rows into SensorData objects. */
	  protected RowParser parser;

	/**
	 * Creates a new HneiTabularFileSensor object.
	 * 
	 * @param filename File that contains data for sources.
	 * @param uri URI for WattDepot server.
	 * @param sourceName Source that is described by the sensor data.
	 * @param username Owner of the WattDepot server.
	 * @param password Password to access the WattDepot server.
	 * @param skipFirstRow True if first row contains row headers, false otherwise.
	 */
	public HneiTabularFileSensor(String filename, String uri, String sourceName,
			                     String username, String password, boolean skipFirstRow) {
	    this.filename = filename;
	    this.serverUri = uri;
	    this.sourceName = sourceName;
	    this.username = username;
	    this.password = password;
	    this.skipFirstRow = skipFirstRow;
		this.parser = new HneiCsvRowParser("HneiTabularFileSensor", this.serverUri, this.sourceName);
	}

    /**
     * Stores a source on a WattDepot server if it does not exist yet and then stores sensor data
     * for that source.
     * 
     * @param client WattDepotClient used to connect to the WattDepot server.
     * @param source Source that is described by the sensor data.
     * @param datum Sensor data for a source.
     * @param sources List of all sources currently on the WattDepot server.
     * @return
     */
	public boolean process(WattDepotClient client, Source source, SensorData datum, List<Source> sources) {
		try {
			try {
				client.storeSource(source, false);
			}
			catch (OverwriteAttemptedException e) {
				System.err.println("Source already exists on server.");
			}
			catch (Exception e) {
				System.err.println(e.toString());
			}
		    client.storeSensorData(datum);
		}
		catch (OverwriteAttemptedException e) {
			System.err.println("Data at " + datum.getTimestamp().toString() + " already exists on server.");
		}
		catch (Exception e) {
			System.err.println(e.toString());
		}
		return true;
	}

	/**
	 * Given a CSV file with lots of sources, this program will parse each row, which represents a
	 * source; create a SensorData object from each row, and store the sensor data for the source
	 * on a WattDepot server.
	 * 
	 * @param args Contains filename, server URI, username, and password.
	 */
	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Command-line arguments not in correct format. Exiting...");
			System.exit(1);
		}

		String filename  = args[0];
		String serverUri = args[1];
		String username  = args[2];
		String password  = args[3];

		// Open CSV file for reading.
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(filename), ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
		} catch (FileNotFoundException e) {
			System.err.println("File not found! Exiting...");
			System.exit(1);
		}

		// Grab data from CSV file.
		HneiTabularFileSensor inputClient = null;
		WattDepotClient client = new WattDepotClient(serverUri, username, password);

		try {
			List<Source> sources = null;
			try {
				sources = client.getSources();
			} catch (Exception e) {
				System.err.println(e.toString());
			}

			String source = null;
			String[] line = null;
			while ((line = reader.readNext()) != null) {
				// Each line contains a possibly different source, so create a new HneiTabularFileSensor object each time.
				line   = reader.readNext();
				source = line[0];

				inputClient = new HneiTabularFileSensor(filename, serverUri, source, username, password, true);
				try {
					inputClient.process(client, new Source(source, username, true), inputClient.parser.parseRow(line), sources);
				} catch (RowParseException e) {
					System.err.println(e.toString());
				}
			}
		}
		catch (IOException e) {
			System.err.println("There was a problem reading in the input file:\n" + e.toString() + "\n\nExiting...");
			System.exit(1);
		}

		// Print list of sources.
		try {
			for (Source src : client.getSources()) {
				System.out.println(src.toString());	
			}
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}

}
