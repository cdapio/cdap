package com.continuuity.machinedata;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.machinedata.data.CPUStat;
import com.continuuity.machinedata.data.CPUStatsTable;
import org.slf4j.LoggerFactory;

public class CPUStatsWriterFlowlet extends AbstractFlowlet {

  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsWriterFlowlet.class);

  public static final String NAME = "CPU_STATS_WRITER_FLOWLET";
  public static final String DESC = "CPU stats writer flowlet";

  @UseDataSet(MachineDataApp.CPU_STATS_TABLE)
  CPUStatsTable cpuStatsTable;

  @ProcessInput(CPUStatsParserFlowlet.OUTPUT_NAME)
  public void process(CPUStat cpuStat) throws OperationException {

    if (cpuStat != null) {
      // write log entry
      this.cpuStatsTable.write(cpuStat);
      LOG.info("writing stats: " + cpuStat.timesStamp + ", " + cpuStat.hostname + ", " + cpuStat.cpuUsage);
    }
  }
}