package com.continuuity.machinedata.query;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.machinedata.CPUStatsParserFlowlet;
import com.continuuity.machinedata.MachineDataApp;
import com.continuuity.machinedata.data.CPUStat;
import com.continuuity.machinedata.data.CPUStatsTable;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class CPUStatsProcedure extends AbstractProcedure {
  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsProcedure.class);

  @UseDataSet(MachineDataApp.NAME)
  CPUStatsTable statsTable;

  @Handle("getStats")
  public void getStats(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    Map<String, String> args = request.getArguments();

    if (args.size() < 3) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Wrong parameters. Please Provide <hostname>, <timestamp_from>, <timestamp_to>");
    }

    String hostname = request.getArgument("hostname");

    long ts_from = Long.getLong(request.getArgument("timestamp_from"));
    long ts_to = Long.getLong(request.getArgument("timestamp_to"));

    List<CPUStat> stats = statsTable.read(hostname, ts_from, ts_to);
    LOG.info("Number of stats found: " + stats.size());

    if (stats.size() != 0)      {
      for (CPUStat stat  : stats) {
        LOG.warn("Stats found: " + stat.hostname + ", " + stat.cpuUsage + ", " + stat.timesStamp );
        responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), stat);
      }
    }
    else {
      responder.error(ProcedureResponse.Code.NOT_FOUND,"No entries found");
    }
  }
}
