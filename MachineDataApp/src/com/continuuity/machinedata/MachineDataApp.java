package com.continuuity.machinedata;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.machinedata.query.CPUStatsProcedure;

/**
 *
 */
public class MachineDataApp implements Application {
  public static final String NAME = "MachineDataApp";
  public static final String DESC = "Machine Data collection App";

  /**
   * Name of the input stream carrying JSON formatted Lish social actions.
   */
  public static final String CPU_STATS_STREAM = "cpuStatsStream";

  /* Tables */
  public static final String CPU_STATS_TABLE = "cpu_stats_table";

  @Override
  public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
      .setName(NAME)
      .setDescription(DESC)
      .withStreams()
      .add(new Stream(CPU_STATS_STREAM))
      .withDataSets()
      .add(new SimpleTimeseriesTable(CPU_STATS_TABLE))
      .withFlows()
      .add(new MachineDataFlow())
      .withProcedures()
      .add(new CPUStatsProcedure())
      .build();
  }
}
