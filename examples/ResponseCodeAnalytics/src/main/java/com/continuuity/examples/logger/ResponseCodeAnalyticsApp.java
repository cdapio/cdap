/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.logger;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ResponseCodeAnalyticsApp extends AbstractApplication {
  // The constant to define the row key of a table
  private static final byte [] ROW_KEY = Bytes.toBytes("status");

  @Override
  public void configure() {
    setName("ResponseCodeAnalytics");
    setDescription("HTTP response code analytics");
    // Ingest data into the app via Streams
    addStream(new Stream("logEventStream"));
    // Store processed data in DataSets
    createDataset("statusCodeTable", Table.class);
    // Process log events in real-time using Flows
    addFlow(new LogAnalyticsFlow());
    // Query the processed data using Procedures
    addProcedure(new StatusCodeProcedure());
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
        // Processing logic is written in Flowlets
        .withFlowlets()
          .add("parser", new LogEventParseFlowlet())
          .add("counter", new LogCountFlowlet())
        // Wire the Flowlets into a DAG
        .connect()
           // Data that is sent to the Stream is sent to the parser Flowlet
          .fromStream("logEventStream").to("parser")
           // After parsing, the data is sent to the counter Flowlet
          .from("parser").to("counter")
        .build();
    }
  }

  /**
   * Parse the status code from the log event and emit it to the next Flowlet.
   */
  public static class LogEventParseFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(LogEventParseFlowlet.class);

    private static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
      //   IP       id    user      date          request     code     size    referrer    user agent
      "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");
     // Emitter for emitting status code to the next Flowlet
    @Output("statusCode")
    private OutputEmitter<Integer> output;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void process(StreamEvent event) throws CharacterCodingException {
      // Get a log evnet in String format from a StreamEvent instance
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      LOG.info("Event received: {}", log);

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
    private static final Logger LOG = LoggerFactory.getLogger(LogCountFlowlet.class);

    // Annotation indicates the statuscodes Dataset is used in the Flowlet
    @UseDataSet("statusCodeTable")
    private Table statusCodes;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void count(Integer status) {

      LOG.info("Status code received: {}", status);

      // Increment the number of occurrences of the status code by 1
      statusCodes.increment(ResponseCodeAnalyticsApp.ROW_KEY, Bytes.toBytes(status), 1L);
    }
  }

  /**
   * LogProcedure is used to serve processed log analysis results.
   */
  public static class StatusCodeProcedure extends AbstractProcedure {
    // Annotation indicates that the log-counters Dataset is used in the Procedure
    @UseDataSet("statusCodeTable")
    private Table statusCodes;

    // Annotation indicates that this method is a Procedure handler
    @Handle("getCounts")
    public void getCounts(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      Map<Integer, Long> statusCountMap = new HashMap<Integer, Long>();
      // Get the row using the row key
      Row row = statusCodes.get(ResponseCodeAnalyticsApp.ROW_KEY);

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
