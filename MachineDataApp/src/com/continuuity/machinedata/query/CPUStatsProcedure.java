package com.continuuity.machinedata.query;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable.Entry;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.machinedata.MachineDataApp;
import com.google.gson.Gson;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CPUStatsProcedure extends AbstractProcedure {
  private org.slf4j.Logger LOG = LoggerFactory.getLogger(CPUStatsProcedure.class);

  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  @UseDataSet(MachineDataApp.CPU_STATS_TABLE)
  SimpleTimeseriesTable timeSeriesTable;

  @Handle("getStats")
  public void getStats(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    Map<String, String> args = request.getArguments();

    if (args.size() < 3) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Wrong parameters. Please Provide <hostname>, <timestamp_from>, <timestamp_to>");
    }

    String hostname = request.getArgument("hostname");
    long ts_from = Long.getLong(request.getArgument("timestamp_from"));
    long ts_to = Long.getLong(request.getArgument("timestamp_to"));

    List<TimeseriesTable.Entry> entries =
      this.timeSeriesTable.read(Bytes.toBytes("cpu"), ts_from, ts_to, /*tag*/Bytes.toBytes(hostname));
    LOG.info("Entries returned: " + entries.size());

    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));

    for (Entry entry : entries) {
      // Convert object to Json and stream
      writer.write(this.gson.get().toJson( new CpuStat(entry)));
    }
  }

  /**
   * Serializer
   */
  private class CpuStat {
    public long timestamp;
    public int cpu;
    public String hostname;

    CpuStat(Entry entry) {
      this.timestamp = entry.getTimestamp();
      this.cpu = Bytes.toInt(entry.getValue());
      this.hostname = Bytes.toString(entry.getTags()[0]);
    }
  }
}
