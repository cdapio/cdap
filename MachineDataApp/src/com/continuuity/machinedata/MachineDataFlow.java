package com.continuuity.machinedata;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;


/**
 *
 */
public class MachineDataFlow implements Flow {
  public static final String NAME = "MachineDataFlow";
  public static final String DESC = "CPU statistics collection flow";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription(NAME)
      .withFlowlets()
      .add(CPUStatsFlowlet.NAME, new CPUStatsFlowlet())
      .connect()
      .fromStream(MachineDataApp.CPU_STATS_STREAM).to(CPUStatsFlowlet.NAME)
      .build();
  }
}
