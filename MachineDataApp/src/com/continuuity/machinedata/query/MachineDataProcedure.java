package com.continuuity.machinedata.query;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
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
import sun.security.provider.SystemSigner;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class MachineDataProcedure extends AbstractProcedure {
  private org.slf4j.Logger LOG = LoggerFactory.getLogger(MachineDataProcedure.class);

  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  public static final String NAME = "CPUStatsProcedure";

  @UseDataSet(MachineDataApp.MACHINE_STATS_TABLE)
  SimpleTimeseriesTable timeSeriesTable;

  /**
   *
   * @param request no parameters
   * @param responder check if procedure is running
   * @throws Exception
   */
  @Handle("echo")
  public void echo(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), "MachineDataProcedure up and running.");
  }

  /**
   * Return Metrics of a given type <cpu, memory, disk> for a given time range
   * @param request  <type>, <hostname>, <timestamp_from>, <timestamp_to>
   * @param responder
   * @throws Exception
   */
  @Handle("getRange")
  public void getRange(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    Map<String, String> args = request.getArguments();

    if (args.size() < 3) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Wrong parameters. Please Provide <hostname>, <timestamp_from>, <timestamp_to>");
    }

    try {
      String metricType = request.getArgument("type");
      String hostname = request.getArgument("hostname");
      Long l_ts_from = Long.parseLong(request.getArgument("timestamp_from"));
      Long l_ts_to = Long.parseLong(request.getArgument("timestamp_to"));

      List<TimeseriesTable.Entry> entries = this.timeSeriesTable.read(Bytes.toBytes(metricType), l_ts_from, l_ts_to, /*tag*/Bytes.toBytes(hostname));
      LOG.info("Entries returned: " + entries.size());

      ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));

      for (Entry entry : entries) {
        // Convert object to Json and stream
        writer.write(this.gson.get().toJson(new Metric(entry)));
      }

    } catch (NumberFormatException ex) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Bad parameters: " + request.getArgument("hostname") + ", "
        + request.getArgument("timestamp_from") + ", " + request.getArgument("timestamp_to"));
    }
  }

  /**
   * Returns metrics for specified type since the last hour
   * @param request parameters: <type>, <hostname>
   * @param responder
   * @throws Exception
   */
  @Handle("getLastHour")
  public void getLastHour(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    Map<String, String> args = request.getArguments();

    if (args.size() < 2) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Wrong parameters. Please Provide <hostname>, <timestamp_from>, <timestamp_to>");
    }

    try {
      String metricType = request.getArgument("type");
      String hostname = request.getArgument("hostname");
      long currentTime = System.currentTimeMillis();


      List<TimeseriesTable.Entry> entries = this.timeSeriesTable.read(Bytes.toBytes(metricType), currentTime - 1000*60*60, currentTime, /*tag*/Bytes.toBytes(hostname));
      LOG.info("Entries returned: " + entries.size());

      ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));

      for (Entry entry : entries) {
        // Convert object to Json and stream
        writer.write(this.gson.get().toJson(new Metric(entry)));
      }

    } catch (NumberFormatException ex) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Bad parameters: " + request.getArgument("hostname") + ", "
        + request.getArgument("timestamp_from") + ", " + request.getArgument("timestamp_to"));
    }
  }

  /**
   *
   * @param request <none></none>
   * @param responder returns a list of hostnames currently monitored
   * @throws Exception
   */
  @Handle("getHostNames")
  public void getHostnames(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    // TODO: Figure if possible to scan a time series to retrieve all tags without doing a full table scan
    // TODO: Use plain table instead? P

  }



  /**
   * Used to serialize metric
   */
  private class Metric {
    public long timestamp;
    public long value;

    Metric(Entry entry) {
      this.timestamp = entry.getTimestamp();
      this.value = Bytes.toInt(entry.getValue());
    }
  }
}
