package com.continuuity.examples.logger;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Application that analyze Apache access logs to count the occurrences of each HTTP status code.
 */
public class AccessLogApp implements Application {
  // The constant to define the row key of a table
  private static final byte [] ROW_STATUS = Bytes.toBytes("status");

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("accesslog")
      .setDescription("access log analysis app")
      // Ingest data into the app via Streams
      .withStreams()
        .add(new Stream("log-events"))
      // Store processed data in Datasets
      .withDataSets()
        .add(new Table("log-counters"))
      // Process log events in real-time using flows
      .withFlows()
        .add(new LogAnalyticsFlow())
      // Query the processed data using procedures
      .withProcedures()
        .add(new LogProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * Flows are used to perform real-time processing on Apache log events.
   * Flowlets are the building blocks of the flow. Flowlets are wired into a Directed Acyclic graph.
   * LogAnalyticsFlow consists of two flowlets:
   * - LogEventParse: parse status code from log event.
   * - LogCount: aggregate the counters of logs by the status code.
   */
  public static class LogAnalyticsFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("log-analytics-flow")
        .setDescription("Analyze web access log")
        // processing logic is written in flowlets
        .withFlowlets()
          .add("parser", new LogEventParse())
          .add("counter", new LogCount())
        // wire the flowlets into a DAG
        .connect()
           // data that is sent to the stream is sent to the parser flowlet
          .fromStream("log-events").to("parser")
           // after the parsing the data is sent to counter flowlet
          .from("parser").to("counter")
        .build();
    }
  }

  /**
   * Parse status code from log event and emit to the next flowlet.
   */
  public static class LogEventParse extends AbstractFlowlet {
    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
      //   IP       id    user      date          request     code     size    referrer    user agent
      "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
     // Emitter for emitting status code to the next flowlet.
    private OutputEmitter<Integer> output;

    // Annotation indicates that this method can process incoming data.
    @ProcessInput
    public void processFromStream(StreamEvent event) throws CharacterCodingException {
      // Get a log in String format from a StreamEvent object.
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      // Grab status code from a log.
      Matcher matcher = ACCESS_LOG_PATTERN.matcher(log);
        if (matcher.matches() && matcher.groupCount() >= 6) {
          // Emit status code to next connected flowlet.
          output.emit(Integer.parseInt(matcher.group(6)));
        }
    }
  }

  /**
   * Aggregate the counter of logs for each status code in a table.
   */
  public static class LogCount extends AbstractFlowlet {
    // Annotation indicates the dataset, log-counters, is used in the flowlet.
    @UseDataSet("log-counters")
    private Table logCounters;

    // Annotation indicates that this method can process incoming data.
    @ProcessInput
    public void count(Integer status) {
      // logCounter table contains a mapping between status code and number of occurrences.
      logCounters.increment(AccessLogApp.ROW_STATUS, Bytes.toBytes(status), 1L);
    }
  }

  /**
   * LogProcedure is used to serve processed log processing results.
   */
  public static class LogProcedure extends AbstractProcedure {
    // Annotation indicates that the dataset, log-counters, is used in the procedure.
    @UseDataSet("log-counters")
    private Table logCounters;

    @Handle("get-counts")
    public void getCounts(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      Map<Integer, Long> statusCountMap = new HashMap<Integer, Long>();
      // Get a row by the row key.
      Row row = logCounters.get(AccessLogApp.ROW_STATUS);

      if (row != null) {
        // Get all status codes.
        for (Map.Entry<byte[], byte[]> colValue : row.getColumns().entrySet()) {
          statusCountMap.put(Bytes.toInt(colValue.getKey()), Bytes.toLong(colValue.getValue()));
        }
      }
      // Send response with Json format.
      responder.sendJson(statusCountMap);
    }
  }
}
