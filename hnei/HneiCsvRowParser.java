package hnei;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.xml.datatype.XMLGregorianCalendar;

import org.wattdepot.datainput.RowParser;
import org.wattdepot.datainput.RowParseException;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Used to parse CSV files containing sensor data for sources provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvRowParser extends RowParser {

	/** Formats dates that are in the format MM/DD/YYYY hh/mm/ss (A.M.|P.M.). */
	private SimpleDateFormat formatDateTime;

	/** Formats dates that are in the format MM/DD/YYYY. */
	private SimpleDateFormat formatDate;

	/**
	 * Creates a new HneiCsvRowParser object.
	 * 
	 * @param toolName Name of the program.
	 * @param serverUri URI of WattDepot server.
	 * @param sourceName Source that is described by the sensor data.
	 */
	public HneiCsvRowParser(String toolName, String serverUri, String sourceName) {
		super(toolName, serverUri, sourceName);
		this.formatDateTime = new SimpleDateFormat("hh/MM/yyyy hh:mm:ss a", Locale.US);
		this.formatDate = new SimpleDateFormat("hh/MM/yyyy", Locale.US);
	}

	/**
	 * Parses a row of data for a source from a CSV file provided by HNEI.
	 * 
	 * @param col Row of a CSV file that contains data for one source.
	 * @throws RowParseException if timestamps are not in the proper formats.
	 */
	@Override
	public SensorData parseRow(String[] col) throws RowParseException {
		if (col == null) {
			System.err.println("No input row specified.");
			return null;
		}

		// If some values are missing or no readings were taken, ignore row.

		// Sample row: (comma-delimited)
		// 126580270205 , 10/8/2009 9:48:35 PM , 2144948 , 2 , 215 , 13248 , 13248 , 1/13/2011 8:09:35 AM , -152

		if (col.length != 9) {
		    System.err.println("Row not in specified format. Skipping source...");
		    return null;
		}

		if (col[5].equals("No Reading") || col[6].equals("No Reading")) {
			System.err.println("No reading for source: " + col[0].toString());
			return null;
		}

		Properties properties = new Properties();
		// properties.getProperty().add(new Property("account", col[0]));

		Date installDate = null;
		try {
			installDate = formatDateTime.parse(col[1]);
		}
		catch (java.text.ParseException e) {
			try {
				installDate = formatDate.parse(col[1]);
			}
			catch (java.text.ParseException pe) {
				throw new RowParseException("Bad timestamp found in input file: " + installDate, pe);
			}
		}
	    XMLGregorianCalendar installTimestamp = Tstamp.makeTimestamp(installDate.getTime());
	    properties.getProperty().add(new Property("installDate", installTimestamp.toString()));

		properties.getProperty().add(new Property("mtuID",     col[2]));
		properties.getProperty().add(new Property("port",      col[3]));
		properties.getProperty().add(new Property("meterType", col[4]));
		properties.getProperty().add(new Property("rawRead",   col[5]));
		properties.getProperty().add(new Property("reading",   col[6]));

		Date readingDate = null;
		try {
			readingDate = formatDateTime.parse(col[7]);
		}
		catch (java.text.ParseException e) {
			try {
				readingDate = formatDate.parse(col[7]);
			}
			catch (java.text.ParseException pe) {
				throw new RowParseException("Bad timestamp found in input file: " + readingDate, pe);
			}
		}
		properties.getProperty().add(new Property("rssi", col[8]));

	    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(readingDate.getTime());
	    return new SensorData(timestamp, this.toolName, Source.sourceToUri(this.sourceName, this.serverUri), properties);
	}

}
