package org.wattdepot.hnei.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import org.wattdepot.client.BadXmlException;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class will export energy consumed data for one or more sources to a CSV file. The output
 * file is a matrix in which the rows represent sources, the columns represent timestamps, and the
 * cells represent the amount of energy consumed in kWh for a specific time period.
 * 
 * @author BJ Peter DeLaCruz
 */
public class EnergyMatrixExporter extends Exporter {

  /** Header row for matrix. */
  protected List<String> header;

  /** If sampling interval is less than threshold, do not include daily sources in output file. */
  //private static final int SAMPLING_INTERVAL_THRESHOLD = 60 * 24;

  /**
   * Creates a new EnergyMatrixExporter object.
   */
  public EnergyMatrixExporter() {
    this.startTimestamp = null;
    this.endTimestamp = null;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.sources = null;
    this.numSources = 0;
    this.samplingInterval = 0;
    this.formatDateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a", Locale.US);
    this.header = new ArrayList<>();
    this.sources = new ArrayList<>();
  }

  /**
   * Gets the energy consumed for each source between startTimestamp and endTimestamp; data is
   * sampled at the specified sampling interval (in minutes).
   * 
   * @return Data to output to CSV file.
   */
  public String getEnergyData() {
    var msg = "";
    var builder = new StringBuilder();
    var start = this.startTimestamp;
    var end = Tstamp.incrementMinutes(start, this.samplingInterval);

    // msg = "Number of sources: " + this.sources.size() + "\n\n";
    // msg += "The sampling interval is " + this.samplingInterval + " minute(s).\n\n";
    // msg += "Start: ";
    // msg += this.getTimestamp(this.startTimestamp.toGregorianCalendar().getTime().getTime()) +
    // "\n";
    // msg += "End:   ";
    // msg += this.getTimestamp(this.endTimestamp.toGregorianCalendar().getTime().getTime()) +
    // "\n\n";
    // buffer.append(msg);

    // If the sampling interval is less than a certain threshold, don't get energy data for
    // particular sources, e.g. daily sources.
    //if (this.samplingInterval < SAMPLING_INTERVAL_THRESHOLD) {
      //List<Source> temp = new ArrayList<Source>();
      //for (Source s : this.sources) {
        //if (s.getProperty(SamplingInterval.SAMPLING_INTERVAL).equals(SamplingInterval.DAILY)) {
          //temp.add(s);
        //}
      //}
      //this.sources.removeAll(temp);
    //}

    if (!this.sourceDataType.equals(ALL_DATA)) {
      List<Source> temp = new ArrayList<>();
      for (var s : this.sources) {
        if (!s.getProperty(SamplingInterval.SAMPLING_INTERVAL).equals(this.sourceDataType)) {
          temp.add(s);
        }
      }
      this.sources.removeAll(temp);      
    }

    while (Tstamp.lessThan(start, this.endTimestamp)) {
      this.header.add(this.getTimestamp(end.toGregorianCalendar().getTime().getTime()));
      start = Tstamp.incrementMinutes(start, this.samplingInterval);
      end = Tstamp.incrementMinutes(end, this.samplingInterval);
    }
    this.header.add(this.getTimestamp(this.endTimestamp.toGregorianCalendar().getTime().getTime()));
    builder.append(this.getTableHeader());

    start = this.startTimestamp;
    end = Tstamp.incrementMinutes(start, this.samplingInterval);
    try {
      int count = 1;
      for (Source s : this.sources) {
        msg = "\n" + s.getName();
        builder.append(msg);
        while (Tstamp.lessThan(start, this.endTimestamp)) {
          try {
            var data = this.client.getEnergy(s.getName(), start, end, this.samplingInterval);
            msg = "," + this.getInfo(data);
          }
          catch (BadXmlException e) {
            msg = ",N/A";
          }
          builder.append(msg);

          start = Tstamp.incrementMinutes(start, this.samplingInterval);
          end = Tstamp.incrementMinutes(end, this.samplingInterval);
        }
        System.out.print("Finished processing " + count + " out of ");
        System.out.println(this.sources.size() + "...");
        start = this.startTimestamp;
        end = Tstamp.incrementMinutes(start, this.samplingInterval);
        count++;
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }

    return builder.toString();
  }

  /**
   * Prints energy information to a CSV file.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean printData() {
    var today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");
    System.out.println("Generating CSV file...\n");
    System.out.println("Output file: " + today + ".csv\n");

    var outputFile = new File(today + ".csv");
    if (!outputFile.setWritable(true)) {
      return false;
    }

    try (var writer = new BufferedWriter(new FileWriter(outputFile, StandardCharsets.UTF_8))) {
      var result = this.getEnergyData();
      if (result == null) {
        throw new IOException();
      }

      writer.write(result);
      System.out.println(result);
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  /**
   * Returns a table header with names of columns.
   * 
   * @return A table header with names of columns.
   */
  @Override
  public String getTableHeader() {
    var builder = new StringBuilder();
    var msg = "Sources";
    for (var s : this.header) {
      builder.append(msg);
      msg = "," + s;
    }
    return builder.toString();
  }

  /**
   * Returns the amount of energy consumed from the SensorData object in kWh.
   * 
   * @param data SensorData object that contains the amount of energy consumed.
   * @return Energy in kWh.
   */
  @Override
  public String getInfo(SensorData data) {
    var energy = data.getPropertyAsDouble(SensorData.ENERGY_CONSUMED);
    return String.format("%.2f", energy / 1000.0);
  }

  /**
   * Command-line program that will generate a CSV file containing energy information for one or
   * more sources over a given time period and at a given sampling interval.
   * 
   * @param args One argument to specify whether data for all sources should be exported.
   */
  public static void main(String[] args) {
    var getAllSources = args.length == 1 && "-all".equals(args[0]);

    var output = new EnergyMatrixExporter();
    if (!output.setup()) {
      System.exit(1);
    }

    var br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    if (getAllSources && !output.getAllSources()) {
      System.exit(1);
    }
    else if (!getAllSources && (!output.getNumSources(br) || !output.getSourceNames(br))) {
      System.exit(1);
    }

    if (!output.getDates(br) || !output.getSamplingInterval(br)) {
      System.exit(1);
    }

    if (!output.getDataType(br) || !output.printData()) {
      System.exit(1);
    }
  }

}
