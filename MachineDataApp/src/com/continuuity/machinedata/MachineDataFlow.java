package com.continuuity.machinedata;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;


/**
 *
 */
public class MachineDataFlow implements Flow {
  public static final String NAME = "MachineDataFlow";
  public static final String DESC = "Machine collection flow";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription(NAME)
      .withFlowlets()
      .add(CPUStatsFlowlet.NAME, new CPUStatsFlowlet())
      .add(MemStatsFlowlet.NAME, new MemStatsFlowlet())
      .add(DiskStatsFlowlet.NAME, new DiskStatsFlowlet())
      .connect()
      .fromStream(MachineDataApp.CPU_STATS_STREAM).to(CPUStatsFlowlet.NAME)
      .fromStream(MachineDataApp.MEM_STATS_STREAM).to(MemStatsFlowlet.NAME)
      .fromStream(MachineDataApp.DISK_STATS_STREAM).to(DiskStatsFlowlet.NAME)
      .build();
  }
}
