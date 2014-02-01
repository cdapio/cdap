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
 * An Application that analyzes Apache access log events to count the number of occurrences of
 * each HTTP status code.
 */
public class AccessLogApp implements Application {
  // The constant to define the row key of a table
  private static final byte [] ROW_KEY = Bytes.toBytes("status");

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("AccessLogAnalytics")
      .setDescription("HTTP response code analytics")
      // Ingest data into the app via Streams
      .withStreams()
        .add(new Stream("logEventStream"))
      // Store processed data in Datasets
      .withDataSets()
        .add(new Table("statusCodesTable"))
      // Process log events in real-time using Flows
      .withFlows()
        .add(new LogAnalyticsFlow())
      // Query the processed data using Procedures
      .withProcedures()
        .add(new StatusCodeProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * Flows are used to perform real-time processing on Apache log events.
   * Flowlets are the building blocks of the flow. Flowlets are wired into a Directed Acyclic graph.
   * LogAnalyticsFlow consists of two Flowlets:
   * - parser: a LogEventParseFlowlet that parses the status code from a log event.
   * - counter: a LogCountFlowlet that encrements the counters of logs by the status code.
   */
  public static class LogAnalyticsFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("LogAnalyticsFlow")
        .setDescription("Analyze Apache access log events")
        // processing logic is written in Flowlets
        .withFlowlets()
          .add("parser", new LogEventParseFlowlet())
          .add("counter", new LogCountFlowlet())
        // wire the Flowlets into a DAG
        .connect()
           // data that is sent to the Stream is sent to the parser Flowlet
          .fromStream("logEventStream").to("parser")
           // after parsing, the data is sent to the counter Flowlet
          .from("parser").to("counter")
        .build();
    }
  }

  /**
   * Parse the status code from the log event and emit it to the next Flowlet.
   */
  public static class LogEventParseFlowlet extends AbstractFlowlet {
    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
      //   IP       id    user      date          request     code     size    referrer    user agent
      "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
     // Emitter for emitting status code to the next Flowlet
    private OutputEmitter<Integer> output;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void process(StreamEvent event) throws CharacterCodingException {
      // Get a log evnet in String format from a StreamEvent instance
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      // Grab the status code from the log event
      Matcher matcher = ACCESS_LOG_PATTERN.matcher(log);
        if (matcher.matches() && matcher.groupCount() >= 6) {
          // Emit the status code to the next connected Flowlet
          output.emit(Integer.parseInt(matcher.group(6)));
        }
    }
  }

  /**
   * Aggregate the count for each status code in a Table.
   */
  public static class LogCountFlowlet extends AbstractFlowlet {
    // Annotation indicates the statuscodes Dataset is used in the Flowlet
    @UseDataSet("statusCodesTable")
    private Table statusCodes;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void count(Integer status) {
      // Increment the number of occurrences of the status code by 1
      statusCodes.increment(AccessLogApp.ROW_KEY, Bytes.toBytes(status), 1L);
    }
  }

  /**
   * LogProcedure is used to serve processed log analysis results.
   */
  public static class StatusCodeProcedure extends AbstractProcedure {
    // Annotation indicates that the log-counters Dataset is used in the Procedure
    @UseDataSet("statusCodesTable")
    private Table statusCodes;

    // Annotation indicates that this method is a Procedure handler
    @Handle("getCounts")
    public void getCounts(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      Map<Integer, Long> statusCountMap = new HashMap<Integer, Long>();
      // Get the row using the row key
      Row row = statusCodes.get(AccessLogApp.ROW_KEY);

      if (row != null) {
        // Get the number of occurrences of all the status codes
        for (Map.Entry<byte[], byte[]> colValue : row.getColumns().entrySet()) {
          statusCountMap.put(Bytes.toInt(colValue.getKey()), Bytes.toLong(colValue.getValue()));
        }
      }
      // Send response in JSON format
      responder.sendJson(statusCountMap);
    }
  }
}
