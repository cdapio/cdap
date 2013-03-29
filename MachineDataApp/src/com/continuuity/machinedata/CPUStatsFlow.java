package com.continuuity.machinedata;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import au.com.bytecode.opencsv.CSVParser;

import java.io.IOException;
import java.util.logging.Logger;

import com.continuuity.machinedata.data.CPUStat;
import com.continuuity.machinedata.data.CPUStatsTable;
import org.slf4j.LoggerFactory;

import com.continuuity.machinedata.CPUStatsParserFlowlet;


/**
 *
 */
public class CPUStatsFlow implements Flow {
  public static final String NAME = "CPUStatsFlow";
  public static final String DESC = "CPU statistics collection flow";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription(NAME)
      .withFlowlets()
      .add(CPUStatsParserFlowlet.NAME, new CPUStatsParserFlowlet())
      .add(CPUStatsWriterFlowlet.NAME, new CPUStatsWriterFlowlet())
      .connect()
      .fromStream(MachineDataApp.CPU_STATS_STREAM).to(CPUStatsParserFlowlet.NAME)
      .from(CPUStatsParserFlowlet.NAME).to(CPUStatsWriterFlowlet.NAME)
      .build();
  }
}
