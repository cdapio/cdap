package com.continuuity.machinedata;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.data.dataset.TimeseriesTable.Entry;
import java.util.Date;

/**
 *
 */
public class MetricsCollector extends AbstractFlowlet {

  @UseDataSet(MachineDataApp.MACHINE_STATS_TABLE)
  SimpleTimeseriesTable timeSeriesTable;

  @ProcessInput
  public void process(StreamEvent event) throws OperationException {
    String eventString = new String(Bytes.toBytes(event.getBody()));
    Entry entry = this.generateEntry(eventString);

    if (entry != null) {
      // Write to time series table
      try {
        timeSeriesTable.write(entry);
      } catch (OperationException ex) {
        throw ex;
      }
    }
  }

  /**
   *
   * @param metric
   * @return returns the corresponding time series metric entry
   */
  public Entry generateEntry(String metric) {
    return null;
  }
}
