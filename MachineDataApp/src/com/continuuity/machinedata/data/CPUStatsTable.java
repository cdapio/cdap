package com.continuuity.machinedata.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable.Entry;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.data.dataset.table.Table;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CPUStatsTable extends DataSet {
  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsTable.class);

  SimpleTimeseriesTable timeSeriesTable;
  KeyValueTable kvTable;


  public CPUStatsTable(String name) {
    super(name);
    this.timeSeriesTable = new SimpleTimeseriesTable(name);
    this.kvTable = new KeyValueTable("kv_" + name);
  }

  public CPUStatsTable(DataSetSpecification spec) {
    super(spec);
    this.timeSeriesTable = new SimpleTimeseriesTable(getName());
    this.kvTable = new KeyValueTable("kv_" + getName());
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .dataset(this.timeSeriesTable.configure())
      .create();
  }

  public void write(CPUStat stat) {

    // create time series entry
    Entry entry = new Entry(Bytes.toBytes("cpu"), Bytes.toBytes(stat.cpuUsage), stat.timesStamp, /*tag=hostname*/Bytes.toBytes(stat.hostname));
    LOG.info("Writing to dataset: " + stat.hostname + ", " + stat.cpuUsage + ", " + stat.timesStamp);

    // Write to time series table
    try {
    timeSeriesTable.write(entry);
    } catch (OperationException ex) {
      LOG.error("Unable to write entry.");
    }
  }

  /**
   *
   * @param hostname
   * @return List of all cpu stats for given host
   */
  public ArrayList<CPUStat> read(String hostname, long ts_from, long ts_to) {

    ArrayList<CPUStat> cpuStatList = new ArrayList<CPUStat>();

    try {
      List<Entry> entries = this.timeSeriesTable.read(Bytes.toBytes("cpu"), ts_from, ts_to, /*tag*/Bytes.toBytes(hostname));
      LOG.warn("Entries returned: " + entries.size());

      for (Entry entry : entries) {
        // convert to CPU stats
        cpuStatList.add(new CPUStat(entry.getTimestamp(), Bytes.toInt(entry.getValue()), new String(entry.getKey())));
      }
    } catch (OperationException e) {
    }

    return cpuStatList;
  }


  /**
   *
   *
   */
  public void setAttribute(String key, String value) {

    try {
      this.kvTable.write(key.getBytes(), value.getBytes());
    } catch (OperationException e) {

    }
  }

  /*
   *
   *
   */
  public String getAttribute(String key) {
    String sValue = null;

    try {
      sValue = new String(this.kvTable.read(key.getBytes()));
    } catch (OperationException ex) {
    }

    return sValue;
  }

  public byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
  }

  public long bytesToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes);
    buffer.flip();//need flip
    return buffer.getLong();
  }
}
